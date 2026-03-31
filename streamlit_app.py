import io
import zipfile
from datetime import date

import pandas as pd
import streamlit as st

from dosespot_processing import compute_usage, chunk_csv_bytes, map_billing_files_to_customers
from tabs_api import TabsClient, bulk_attach_billing_to_invoices, build_invoice_mapping


st.set_page_config(page_title="Dosespot Usage Uploader", page_icon="💊", layout="wide")


def read_uploaded_csv(file) -> pd.DataFrame:
    try:
        return pd.read_csv(file)
    except Exception:
        # Retry with alternative line terminator used in source script
        try:
            file.seek(0)
            return pd.read_csv(file, lineterminator='\r')
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")
            return pd.DataFrame()


def read_uploaded_excel(file) -> pd.DataFrame:
    try:
        # Explicitly use openpyxl for .xlsx; pandas will pick engine if available
        return pd.read_excel(file, engine=None)
    except Exception as e:
        st.error(f"Failed to read Excel: {e}")
        return pd.DataFrame()


def read_billing_zip(file) -> list[tuple[str, pd.DataFrame]]:
    """Read CSVs from a ZIP, returning (filename, DataFrame) pairs."""
    entries: list[tuple[str, pd.DataFrame]] = []
    try:
        with zipfile.ZipFile(file) as zf:
            for name in zf.namelist():
                if name.lower().endswith('.csv'):
                    try:
                        data = zf.read(name)
                        try:
                            df = pd.read_csv(io.BytesIO(data))
                        except Exception:
                            df = pd.read_csv(io.BytesIO(data), lineterminator='\r')
                        if not df.empty:
                            entries.append((name, df))
                    except Exception as e:
                        st.warning(f"Could not read {name} from ZIP: {e}")
    except Exception as e:
        st.error(f"Failed to open ZIP: {e}")
    return entries


st.title("Dosespot Usage Upload")
st.write("Upload billing files, IDP data, and the customers CSV to generate usage files.")

col1, col2 = st.columns(2)
with col1:
    billing_zip = st.file_uploader(
        "Billing Files", type=["zip"], accept_multiple_files=False
    )
    idp_file = st.file_uploader("IDP Data", type=["xlsx", "xls"], accept_multiple_files=False)
with col2:
    customers_file = st.file_uploader(
        "Customer Mapping CSV",
        type=["csv"],
        accept_multiple_files=False,
    )
    selected_date = st.date_input("Usage date (YYYY-MM-DD)", value=date.today())

st.divider()

generate_clicked = st.button("Generate Usage Files", type="primary")
st.link_button(
    "Upload Usage",
    "https://app.tabsplatform.com/merchant/usage/all?page=1&sort=uploadTime&sortDir=desc",
    type="primary",
)


if generate_clicked:
    if billing_zip is None or idp_file is None or customers_file is None:
        st.warning("Please upload all required files.")
    else:
        with st.spinner("Processing..."):
            billing_entries = read_billing_zip(billing_zip)
            idp_df = read_uploaded_excel(idp_file)
            customers_df = read_uploaded_csv(customers_file)

            billing_dfs = [df for _, df in billing_entries]

            try:
                output_df = compute_usage(
                    billing_dfs=billing_dfs,
                    idp_df=idp_df,
                    all_customers_df=customers_df,
                    date_str=selected_date.strftime("%Y-%m-%d"),
                )
            except Exception as e:
                st.error(f"Failed to compute usage: {e}")
                output_df = pd.DataFrame()

            st.session_state["usage_output_df"] = output_df
            st.session_state["usage_filename_base"] = f"output-{selected_date.strftime('%Y%m%d')}"
            st.session_state["usage_chunks"] = chunk_csv_bytes(
                output_df=output_df,
                base_output_filename=st.session_state["usage_filename_base"],
                chunk_size=500,
            )
            st.session_state["billing_entries"] = billing_entries
            st.session_state["customers_df"] = customers_df

# Render last generated results if available
output_df_state = st.session_state.get("usage_output_df")
if isinstance(output_df_state, pd.DataFrame):
    if output_df_state.empty:
        st.info("No usage rows generated. Please verify inputs.")
    else:
        st.subheader("Preview")
        st.dataframe(output_df_state.head(50), width='stretch')

        chunks_state = st.session_state.get("usage_chunks", [])
        if chunks_state:
            st.subheader("Downloads")
            for fname, blob in chunks_state:
                st.download_button(
                    label=f"Download {fname}",
                    data=blob,
                    file_name=fname,
                    mime="text/csv",
                )
        else:
            st.info("No non-empty chunks to download.")

st.divider()

# ── Bulk Attach Billing Files to Invoices ─────────────────────────────────────
st.subheader("Bulk Attach Billing Files to Invoices")
st.caption(
    "After generating usage, attach the original billing CSVs to matching "
    "Tabs invoices."
)

tabs_url = "https://integrators.prod.api.tabsplatform.com"
tabs_key = st.text_input("Tabs API Key", type="password")

billing_entries_state = st.session_state.get("billing_entries", [])
customers_df_state = st.session_state.get("customers_df")

attach_ready = (
    bool(tabs_key)
    and len(billing_entries_state) > 0
    and customers_df_state is not None
)

# Step 1 — Invoice Mapping (one API call)
map_clicked = st.button(
    "Map Invoices",
    type="secondary",
    disabled=not attach_ready,
    help="Generate usage first, then provide Tabs API key above.",
)

if map_clicked:
    client = TabsClient(base_url=tabs_url, api_key=tabs_key)
    date_str = selected_date.strftime("%Y-%m-%d")

    try:
        mapped = map_billing_files_to_customers(
            billing_entries_state, customers_df_state, fuzzy_match=True
        )
    except Exception as e:
        st.error(f"Customer mapping failed: {e}")
        mapped = []

    if mapped:
        with st.spinner(f"Fetching invoices for {date_str}…"):
            invoices_cache = client.fetch_all_invoices_by_date(date_str)

        invoice_mapping = build_invoice_mapping(mapped, invoices_cache, date_str)
        st.session_state["mapped_entries"] = mapped
        st.session_state["invoices_cache"] = invoices_cache
        st.session_state["invoice_mapping"] = invoice_mapping

mapped_state = st.session_state.get("mapped_entries", [])
invoice_mapping_state = st.session_state.get("invoice_mapping", [])

if invoice_mapping_state:
    ready = [r for r in invoice_mapping_state if r["mapping_status"] == "Ready"]
    no_invoice = [r for r in invoice_mapping_state if r["mapping_status"] == "No invoice found"]
    no_customer = [r for r in invoice_mapping_state if r["mapping_status"] == "No customer match"]
    fuzzy = [e for e in mapped_state if e.get("match_type") == "fuzzy"]

    st.markdown(
        f"**{len(ready)}** ready to attach, "
        f"**{len(no_invoice)}** no invoice found, "
        f"**{len(no_customer)}** no customer match"
    )

    # Invoice mapping table
    st.subheader("Invoice Mapping")
    mapping_display = pd.DataFrame([
        {
            "Filename": r["filename"],
            "Client Name": r["client_name"],
            "Customer Name": r["tabs_customer_name"],
            "Invoice ID": r["invoice_id"] or "—",
            "Invoice Status": r["invoice_status"] or "—",
            "Match Type": r["match_type"],
            "Status": r["mapping_status"],
        }
        for r in invoice_mapping_state
        if r["mapping_status"] != "No customer match"
    ])
    st.dataframe(mapping_display, use_container_width=True)

    if fuzzy:
        st.subheader("Fuzzy Matches — Review")
        st.caption(
            "These billing files matched by name, not Client ID. "
            "Confirmed matches will have the Client ID custom field set on the Tabs customer."
        )
        fuzzy_review = pd.DataFrame([
            {
                "Billing Client Name": e["client_name"],
                "Tabs Customer Name": e["tabs_customer_name"],
                "Score": e["match_score"],
                "Client ID": e["client_id"],
                "Filename": e["filename"],
            }
            for e in fuzzy
        ])
        st.data_editor(
            fuzzy_review,
            column_config={"_select": st.column_config.CheckboxColumn("Include", default=True)},
            disabled=["Billing Client Name", "Tabs Customer Name", "Score", "Client ID", "Filename"],
            num_rows="fixed",
            use_container_width=True,
            key="fuzzy_review_editor",
        )

    if no_customer:
        with st.expander(f"Unmatched Files ({len(no_customer)})", expanded=False):
            st.caption("These billing files could not be matched to any Tabs customer.")
            st.dataframe(
                pd.DataFrame([
                    {"Client Name": r["client_name"], "Client ID": r["client_id"], "Filename": r["filename"]}
                    for r in no_customer
                ]),
                use_container_width=True,
            )

    # Step 2 — Bulk attach
    attach_clicked = st.button(
        f"Bulk Attach {len(ready)} Files to Invoices",
        type="primary",
        disabled=len(ready) == 0,
    )

    if attach_clicked:
        client = TabsClient(base_url=tabs_url, api_key=tabs_key)

        # Resolve Client ID custom field for backfill
        client_id_field_id = None
        if fuzzy:
            with st.spinner("Looking up Client ID custom field…"):
                client_id_field_id = client.resolve_client_id_field()
            if not client_id_field_id:
                st.warning("Could not find a 'Client ID' custom field — fuzzy matches will attach but won't backfill the field.")

        # Filter out rejected fuzzy matches
        if fuzzy and "fuzzy_review_editor" in st.session_state:
            editor_state = st.session_state["fuzzy_review_editor"]
            deleted_rows = {r for r in editor_state.get("deleted_rows", [])}
            approved_fuzzy_filenames = {
                fuzzy_review.iloc[i]["Filename"]
                for i in range(len(fuzzy_review))
                if i not in deleted_rows
            }
            final_entries = [
                e for e in mapped_state
                if e["match_type"] == "client_id"
                or (e["match_type"] == "fuzzy" and e["filename"] in approved_fuzzy_filenames)
            ]
            final_mapping = [
                r for r in invoice_mapping_state
                if r["mapping_status"] == "Ready" and (
                    r["match_type"] == "client_id"
                    or r["filename"] in approved_fuzzy_filenames
                )
            ]
        else:
            final_entries = [e for e in mapped_state if e["match_type"] != "unmatched"]
            final_mapping = [r for r in invoice_mapping_state if r["mapping_status"] == "Ready"]

        if not final_mapping:
            st.warning("No billing files to attach.")
        else:
            progress = st.progress(0, text="Attaching…")

            def _update_progress(idx: int, total: int, fname: str):
                progress.progress((idx + 1) / total, text=f"({idx + 1}/{total}) {fname}")

            results = bulk_attach_billing_to_invoices(
                client=client,
                mapped_entries=final_entries,
                invoice_mapping=final_mapping,
                client_id_field_id=client_id_field_id,
                progress_callback=_update_progress,
            )
            progress.empty()

            results_df = pd.DataFrame(results)
            attached = results_df[results_df["status"] == "attached"]
            errored = results_df[results_df["status"] == "error"]
            backfilled = results_df[results_df.get("client_id_set", False) == True] if "client_id_set" in results_df.columns else pd.DataFrame()

            summary = f"Done — {len(attached)} attached, {len(errored)} errors"
            if not backfilled.empty:
                summary += f", {len(backfilled)} Client IDs set"
            st.success(f"{summary}.")
            if not results_df.empty:
                display_cols = [c for c in results_df.columns if c != "df"]
                st.dataframe(results_df[display_cols], use_container_width=True)
