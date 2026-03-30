import re
from difflib import SequenceMatcher
from io import BytesIO
from typing import List, Tuple, Dict, Optional

import pandas as pd


def _normalize_column_name(name: str) -> str:
    """Lowercase and strip non-alphanumeric characters to normalize a column name."""
    if not isinstance(name, str):
        name = str(name)
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _standardize_customer_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename common variants to expected names for the customers mapping file."""
    if df is None or df.empty:
        return df

    normalized_to_actual: Dict[str, str] = { _normalize_column_name(c): c for c in df.columns }

    rename_map: Dict[str, str] = {}

    # Map client id variants
    for variant in [
        "clientid",
        "client_id",
        "clientidnumber",
        "clientnumber",
        "clientno",
        "clientcode",
    ]:
        if variant in normalized_to_actual:
            rename_map[ normalized_to_actual[variant] ] = "Client ID"
            break

    # Map customer id variants
    for variant in [
        "customerid",
        "customer_id",
        "customernumber",
        "customerno",
        "customercode",
        "accountid",
        "account_id",
        "customeridnumber",
    ]:
        if variant in normalized_to_actual:
            rename_map[ normalized_to_actual[variant] ] = "Customer ID"
            break

    # If still missing, map a generic 'ID' column to Customer ID
    if "Customer ID" not in rename_map.values() and "Customer ID" not in df.columns:
        if "id" in normalized_to_actual:
            rename_map[ normalized_to_actual["id"] ] = "Customer ID"

    # Map included prescribers variants
    for variant in ["includedprescribers", "included_prescribers", "includedproviders", "included_provider_count"]:
        if variant in normalized_to_actual:
            rename_map[ normalized_to_actual[variant] ] = "Included Prescribers"
            break

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def compute_usage(
    billing_dfs: List[pd.DataFrame],
    idp_df: pd.DataFrame,
    all_customers_df: pd.DataFrame,
    date_str: str,
) -> pd.DataFrame:
    """
    Compute usage rows from uploaded billing CSV data, IDP data, and customers mapping.

    Returns a dataframe with columns:
      - customer_id
      - event_type_name in {"Rx", "EPCS", "Non-EPCS", "Agents", "Free", "IDP", "MedHistory Reconciliation"}
      - datetime (string YYYY-MM-DD)
      - value (int)
      - differentiator (None)
    """

    # Normalize/standardize customer column names and ensure defaults
    all_customers_df = _standardize_customer_columns(all_customers_df)
    # Validate required columns exist after normalization
    missing_cols = [c for c in ["Client ID", "Customer ID"] if c not in all_customers_df.columns]
    if missing_cols:
        raise ValueError(
            f"Customers mapping file is missing required column(s): {', '.join(missing_cols)}. "
            f"Detected columns: {list(all_customers_df.columns)}"
        )
    if "Included Prescribers" not in all_customers_df.columns:
        all_customers_df["Included Prescribers"] = 10

    # IDP counts per clientid
    # Normalize IDP dataframe column names
    if not idp_df.empty:
        # Create normalized column map for IDP data
        idp_normalized = {_normalize_column_name(c): c for c in idp_df.columns}
        
        # Find clientid column variant
        clientid_col = None
        for variant in ["clientid", "client_id", "clientidnumber", "clientnumber"]:
            if variant in idp_normalized:
                clientid_col = idp_normalized[variant]
                break
        
        # Find clinicianid column variant
        clinicianid_col = None
        for variant in ["clinicianid", "clinician_id", "providerid", "provider_id", "doctorid"]:
            if variant in idp_normalized:
                clinicianid_col = idp_normalized[variant]
                break
        
        if clientid_col and clinicianid_col:
            idp_counts = idp_df.groupby(clientid_col)[clinicianid_col].nunique().reset_index()
            idp_counts.columns = ["clientid", "IDP"]
        else:
            idp_counts = pd.DataFrame(columns=["clientid", "IDP"])
    else:
        idp_counts = pd.DataFrame(columns=["clientid", "IDP"])

    output_rows = []

    required_columns = [
        "Client ID",
        "Number of Prescriptions",
        "Role",
        "EPCS (enabled)",
    ]

    for billing_df in billing_dfs:
        if billing_df is None or billing_df.empty:
            continue

        # Validate columns
        if not set(required_columns).issubset(billing_df.columns):
            # Skip files missing required columns
            continue

        # Aggregate metrics from the billing file
        rx_sum = billing_df["Number of Prescriptions"].sum()
        medhistory_recon_sum = (
            billing_df["MedHistory Reconciliation Request Count"].sum()
            if "MedHistory Reconciliation Request Count" in billing_df.columns
            else 0
        )
        epcs_count = billing_df[
            (billing_df["Role"] == "PrescribingClinician")
            & (billing_df["EPCS (enabled)"] == True)
        ].shape[0]
        non_epcs_count = billing_df[
            (billing_df["Role"] == "PrescribingClinician")
            & (billing_df["EPCS (enabled)"] == False)
        ].shape[0]
        agents_count = billing_df[
            billing_df["Role"] == "PrescribingAgentClinician"
        ].shape[0]

        # Client ID can be numeric; fallback to Clinic(s) if needed
        client_id = billing_df.iloc[0]["Client ID"]
        try:
            client_id = int(client_id)
        except Exception:
            if "Clinic(s)" in billing_df.columns:
                client_id = billing_df.iloc[0]["Clinic(s)"]

        # Map to customer
        customer_row = all_customers_df[all_customers_df["Client ID"] == client_id]
        if customer_row.empty:
            # No mapping for this client; skip this file
            continue

        customer_id = customer_row.iloc[0]["Customer ID"]
        included_prescribers = (
            customer_row.iloc[0]["Included Prescribers"]
            if pd.notnull(customer_row.iloc[0]["Included Prescribers"]) else 10
        )

        # Deduction logic
        free_deduction = 0
        if non_epcs_count >= included_prescribers:
            non_epcs_deducted = included_prescribers
            non_epcs_count -= non_epcs_deducted
            free_deduction += non_epcs_deducted
        else:
            non_epcs_deducted = non_epcs_count
            free_deduction += non_epcs_deducted
            non_epcs_count = 0

            # Deduct the remaining amount from EPCS if Non-EPCS is less
            epcs_deducted = min(included_prescribers - free_deduction, epcs_count)
            epcs_count -= epcs_deducted
            free_deduction += epcs_deducted

        free = free_deduction

        # IDP counts for the client
        idp_count = 0
        if not idp_counts.empty:
            idp_count_row = idp_counts[idp_counts["clientid"] == client_id]
            idp_count = idp_count_row["IDP"].values[0] if not idp_count_row.empty else 0

        # Prepare rows
        results = [
            {
                "customer_id": customer_id,
                "event_type_name": "Rx",
                "datetime": date_str,
                "value": int(rx_sum),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "EPCS",
                "datetime": date_str,
                "value": int(epcs_count),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "Non-EPCS",
                "datetime": date_str,
                "value": int(non_epcs_count),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "Agents",
                "datetime": date_str,
                "value": int(agents_count),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "Free",
                "datetime": date_str,
                "value": int(free),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "IDP",
                "datetime": date_str,
                "value": int(idp_count),
                "differentiator": None,
            },
            {
                "customer_id": customer_id,
                "event_type_name": "MedHistory Reconciliation",
                "datetime": date_str,
                "value": int(medhistory_recon_sum),
                "differentiator": None,
            },
        ]
        output_rows.extend(results)

    output_df = pd.DataFrame([row for row in output_rows if row["value"] != 0])
    return output_df


_STRIP_SUFFIXES = re.compile(
    r"\b(llc|inc|ltd|corp|co|company|corporation|l\.?l\.?c|incorporated|dba)\b",
    re.IGNORECASE,
)


def _normalize_name(name: str) -> str:
    """Lowercase, strip legal suffixes and non-alphanumeric chars for comparison."""
    name = _STRIP_SUFFIXES.sub("", name.lower())
    return re.sub(r"[^a-z0-9]", "", name)


def _fuzzy_find_customer(
    client_name: str,
    customers_df: pd.DataFrame,
    name_col: str = "Name",
    threshold: float = 0.6,
) -> Optional[Tuple[pd.Series, float]]:
    """
    Find the best fuzzy match for *client_name* among rows in *customers_df*.

    Returns (customer_row, score) if the best match exceeds *threshold*,
    otherwise None.
    """
    if name_col not in customers_df.columns:
        return None

    norm_query = _normalize_name(client_name)
    if not norm_query:
        return None

    best_score = 0.0
    best_idx = None

    for idx, cust_name in customers_df[name_col].items():
        if not isinstance(cust_name, str):
            continue
        norm_cust = _normalize_name(cust_name)
        if not norm_cust:
            continue

        if norm_query == norm_cust:
            return customers_df.loc[idx], 1.0

        if norm_query in norm_cust or norm_cust in norm_query:
            score = max(0.85, SequenceMatcher(None, norm_query, norm_cust).ratio())
        else:
            score = SequenceMatcher(None, norm_query, norm_cust).ratio()

        if score > best_score:
            best_score = score
            best_idx = idx

    if best_idx is not None and best_score >= threshold:
        return customers_df.loc[best_idx], best_score
    return None


def map_billing_files_to_customers(
    billing_entries: List[Tuple[str, pd.DataFrame]],
    all_customers_df: pd.DataFrame,
    fuzzy_match: bool = False,
    fuzzy_threshold: float = 0.6,
) -> List[Dict]:
    """
    Map billing file DataFrames to their Tabs customer IDs.

    Each returned dict has keys:
        customer_id, client_id, filename, df, match_type, client_name,
        tabs_customer_name, match_score.

    *match_type* is ``"client_id"`` for exact Client-ID matches or
    ``"fuzzy"`` for name-based fuzzy matches (only attempted when
    *fuzzy_match* is True).
    """
    all_customers_df = _standardize_customer_columns(all_customers_df)

    missing = [c for c in ("Client ID", "Customer ID") if c not in all_customers_df.columns]
    if missing:
        raise ValueError(
            f"Customer mapping missing column(s): {', '.join(missing)}. "
            f"Detected: {list(all_customers_df.columns)}"
        )

    name_col = "Name" if "Name" in all_customers_df.columns else None

    results: List[Dict] = []
    for filename, billing_df in billing_entries:
        if billing_df is None or billing_df.empty:
            continue
        if "Client ID" not in billing_df.columns:
            continue

        client_id = billing_df.iloc[0]["Client ID"]
        try:
            client_id = int(client_id)
        except Exception:
            if "Clinic(s)" in billing_df.columns:
                client_id = billing_df.iloc[0]["Clinic(s)"]

        client_name = str(billing_df.iloc[0].get("Client Name", "")) if "Client Name" in billing_df.columns else ""

        # --- exact Client-ID match ---
        customer_row = all_customers_df[all_customers_df["Client ID"] == client_id]
        if not customer_row.empty:
            row = customer_row.iloc[0]
            results.append({
                "customer_id": str(row["Customer ID"]),
                "client_id": client_id,
                "client_name": client_name,
                "tabs_customer_name": str(row.get("Name", "")),
                "filename": filename,
                "df": billing_df,
                "match_type": "client_id",
                "match_score": 1.0,
            })
            continue

        # --- fuzzy name fallback ---
        if fuzzy_match and client_name and name_col:
            result = _fuzzy_find_customer(
                client_name, all_customers_df, name_col=name_col, threshold=fuzzy_threshold
            )
            if result is not None:
                row, score = result
                results.append({
                    "customer_id": str(row["Customer ID"]),
                    "client_id": client_id,
                    "client_name": client_name,
                    "tabs_customer_name": str(row.get("Name", "")),
                    "filename": filename,
                    "df": billing_df,
                    "match_type": "fuzzy",
                    "match_score": round(score, 3),
                })

    return results


def chunk_csv_bytes(
    output_df: pd.DataFrame, base_output_filename: str, chunk_size: int = 500
) -> List[Tuple[str, bytes]]:
    """
    Split the output dataframe into chunks and return a list of (filename, csv_bytes).
    """
    chunks: List[Tuple[str, bytes]] = []
    if output_df is None or output_df.empty:
        return chunks

    num_chunks = len(output_df) // chunk_size + (
        1 if len(output_df) % chunk_size > 0 else 0
    )
    for i in range(num_chunks):
        chunk = output_df.iloc[i * chunk_size : (i + 1) * chunk_size]
        output_filename = f"{base_output_filename}_{i + 1}.csv"
        buf = BytesIO()
        chunk.to_csv(buf, index=False)
        chunks.append((output_filename, buf.getvalue()))

    return chunks


