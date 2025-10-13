import pandas as pd
from io import BytesIO
from typing import List, Tuple, Dict


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
      - event_type_name in {"Rx", "EPCS", "Non-EPCS", "Agents", "Free", "IDP"}
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
    idp_counts = (
        idp_df.groupby("clientid")["clinicianid"].nunique().reset_index()
        if not idp_df.empty
        else pd.DataFrame(columns=["clientid", "clinicianid"])  # empty fallback
    )
    if not idp_counts.empty:
        idp_counts.columns = ["clientid", "IDP"]

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
        ]
        output_rows.extend(results)

    output_df = pd.DataFrame([row for row in output_rows if row["value"] != 0])
    return output_df


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


