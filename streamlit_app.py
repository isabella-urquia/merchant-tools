import io
import zipfile
from datetime import date

import pandas as pd
import streamlit as st

from dosespot_processing import compute_usage, chunk_csv_bytes


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


def read_billing_zip(file) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
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
                            dfs.append(df)
                    except Exception as e:
                        st.warning(f"Could not read {name} from ZIP: {e}")
    except Exception as e:
        st.error(f"Failed to open ZIP: {e}")
    return dfs


st.title("Dosespot Usage Upload")
st.write("Upload billing files, IDP data, and the customers CSV to generate usage files.")
st.link_button(
    "Upload Usage",
    "https://app.tabsplatform.com/merchant/usage/all?page=1&sort=uploadTime&sortDir=desc",
    type="primary",
)

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
            billing_dfs = read_billing_zip(billing_zip)
            idp_df = read_uploaded_excel(idp_file)
            customers_df = read_uploaded_csv(customers_file)

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

            # Save results in session to persist across reruns
            st.session_state["usage_output_df"] = output_df
            st.session_state["usage_filename_base"] = f"output-{selected_date.strftime('%Y%m%d')}"
            # Precompute chunks for faster subsequent reruns
            st.session_state["usage_chunks"] = chunk_csv_bytes(
                output_df=output_df,
                base_output_filename=st.session_state["usage_filename_base"],
                chunk_size=500,
            )

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


