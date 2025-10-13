## Dosespot Usage Uploader (Streamlit)

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the app

```bash
streamlit run streamlit_app.py
```

### Inputs

- Billing ZIP: A `.zip` containing one or more CSVs. Each CSV must include columns `Client ID`, `Number of Prescriptions`, `Role`, `EPCS (enabled)`.
- IDP Data Excel: An `.xlsx`/`.xls` file with columns `clientid`, `clinicianid`.
- Customers Mapping CSV: Must include columns `Client ID`, `Customer ID`, optional `Included Prescribers` (defaults to 10).

### Output

The app displays a preview and provides downloads:
- Chunked CSV files of up to 500 rows each named like `output-YYYYMMDD_1.csv`, `output-YYYYMMDD_2.csv`, ...
- A ZIP containing all chunked CSVs.


