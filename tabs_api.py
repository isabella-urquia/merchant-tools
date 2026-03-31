import io
import time
from typing import List, Tuple, Dict, Callable, Optional

import pandas as pd
import requests


class TabsClient:
    """Minimal Tabs API client for invoice and attachment operations."""

    def __init__(self, base_url: str, api_key: str, max_retries: int = 10):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"Authorization": api_key})

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(self.max_retries):
            response = self.session.request(method, url, **kwargs)
            if response.status_code == 429:
                time.sleep(attempt + 1)
                continue
            return response
        raise Exception(f"Max retries exceeded for {method} {endpoint}")

    def get_invoices(
        self,
        customer_id: str = None,
        issue_date: str = None,
        status: str = None,
        limit: int = 1000,
    ) -> List[dict]:
        """Fetch invoices with optional filters."""
        filters: list[str] = []
        if customer_id:
            filters.append(f'customerId:eq:"{customer_id}"')
        if issue_date:
            filters.append(f'issueDate:eq:"{issue_date}"')
        if status:
            filters.append(f'status:eq:"{status}"')

        params: dict = {"limit": limit}
        if filters:
            params["filter"] = ",".join(filters)

        response = self._request("GET", "/v3/invoices", params=params)
        result = response.json()
        if not result.get("success"):
            return []

        payload = result.get("payload", {})
        if isinstance(payload, list):
            return payload
        return payload.get("data", [])

    def fetch_all_invoices_by_date(self, issue_date: str, limit: int = 1000) -> List[dict]:
        """Fetch ALL invoices for a given issue date in one paginated pass."""
        all_invoices: List[dict] = []
        page = 1
        while True:
            params = {
                "filter": f'issueDate:eq:"{issue_date}"',
                "limit": limit,
                "page": page,
            }
            response = self._request("GET", "/v3/invoices", params=params)
            result = response.json()
            payload = result.get("payload", {})
            data = payload.get("data", [])
            if not data:
                break
            all_invoices.extend(data)
            total_items = payload.get("totalItems", 0)
            if len(all_invoices) >= total_items:
                break
            page += 1
        return all_invoices

    def get_custom_fields(self) -> List[dict]:
        """Fetch all merchant custom fields (GET /v3/custom-fields)."""
        response = self._request("GET", "/v3/custom-fields")
        result = response.json()
        payload = result.get("payload", [])
        if isinstance(payload, dict):
            return payload.get("data", [])
        return payload if isinstance(payload, list) else []

    def get_customer(self, customer_id: str) -> Optional[dict]:
        """Fetch a single customer by ID (GET /v3/customers/{id})."""
        response = self._request("GET", f"/v3/customers/{customer_id}")
        result = response.json()
        if not result.get("success"):
            return None
        payload = result.get("payload", {})
        return payload.get("data", payload) if isinstance(payload, dict) else payload

    def set_customer_custom_field(
        self, customer_id: str, field_id: str, value: str
    ) -> dict:
        """Set a custom field on a customer (PUT /v3/customers/{id}/custom-field)."""
        payload = [{"manufacturerCustomFieldId": field_id, "customFieldValue": str(value)}]
        response = self._request("PUT", f"/v3/customers/{customer_id}/custom-field", json=payload)
        result = response.json()
        if response.status_code < 200 or response.status_code >= 300:
            msg = result.get("message", result)
            raise Exception(f"Set custom field failed ({response.status_code}): {msg}")
        return result

    def resolve_client_id_field(self) -> Optional[str]:
        """Find the manufacturerCustomFieldId for the 'Client ID' custom field."""
        fields = self.get_custom_fields()
        for f in fields:
            if f.get("name", "").strip().lower() == "client id":
                return f.get("id")
        return None

    def get_customer_custom_field_value(
        self, customer_id: str, field_name: str = None, field_id: str = None
    ) -> Optional[str]:
        """Read a single custom field value from a customer."""
        customer = self.get_customer(customer_id)
        if not customer:
            return None
        for cf in customer.get("customFields", []):
            if field_id and cf.get("manufacturerCustomFieldId") == field_id:
                return cf.get("customFieldValue")
            if field_name and cf.get("customFieldName", "").strip().lower() == field_name.strip().lower():
                return cf.get("customFieldValue")
        return None

    def put_attachment(
        self, invoice_id: str, df: pd.DataFrame, filename: str = "attachment.csv"
    ) -> dict:
        """Attach a CSV file to an invoice (PUT /v16/secrets/invoices/{id}/attachments)."""
        buf = io.BytesIO()
        buf.write(df.to_csv(index=False).encode("utf-8"))
        buf.seek(0)

        files = [("files", (filename, buf, "text/csv"))]
        response = self._request(
            "PUT", f"/v16/secrets/invoices/{invoice_id}/attachments", files=files
        )
        result = response.json()
        if response.status_code < 200 or response.status_code >= 300:
            msg = result.get("message", result)
            raise Exception(f"Attachment failed ({response.status_code}): {msg}")
        return result


def _find_invoice_for_customer(customer_id: str, issue_date: str, invoices_cache: List[Dict]) -> Optional[Dict]:
    """Find the first invoice matching a customer ID from the cached list."""
    for inv in invoices_cache:
        inv_date = inv.get("issueDate", "")[:10]
        if inv.get("customerId") == customer_id and inv_date == issue_date:
            return inv
    return None


def build_invoice_mapping(
    mapped_entries: List[Dict],
    invoices_cache: List[Dict],
    issue_date: str,
) -> List[Dict]:
    """
    Build a mapping table: billing file -> customer -> invoice.

    Uses the pre-fetched *invoices_cache* (from a single API call) so no
    per-customer requests are needed.

    Returns a list of dicts with keys:
        filename, client_name, tabs_customer_name, customer_id, client_id,
        match_type, invoice_id, invoice_status, mapping_status.
    """
    results: List[Dict] = []
    for entry in mapped_entries:
        base = {
            "filename": entry["filename"],
            "client_name": entry.get("client_name", ""),
            "tabs_customer_name": entry.get("tabs_customer_name", ""),
            "customer_id": entry.get("customer_id"),
            "client_id": entry.get("client_id"),
            "match_type": entry.get("match_type", "client_id"),
        }

        if entry.get("match_type") == "unmatched":
            results.append({**base, "invoice_id": None, "invoice_status": None, "mapping_status": "No customer match"})
            continue

        inv = _find_invoice_for_customer(entry["customer_id"], issue_date, invoices_cache)
        if inv:
            results.append({
                **base,
                "invoice_id": inv.get("id"),
                "invoice_status": inv.get("status"),
                "mapping_status": "Ready",
            })
        else:
            results.append({**base, "invoice_id": None, "invoice_status": None, "mapping_status": "No invoice found"})

    return results


def bulk_attach_billing_to_invoices(
    client: TabsClient,
    mapped_entries: List[Dict],
    invoice_mapping: List[Dict],
    client_id_field_id: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict]:
    """
    Bulk-attach billing CSV files using a pre-built invoice mapping.

    Only entries with ``mapping_status == "Ready"`` are processed.
    Fuzzy-matched entries get the Client ID custom field backfilled when
    *client_id_field_id* is provided.
    """
    invoice_lookup = {row["filename"]: row for row in invoice_mapping if row["mapping_status"] == "Ready"}
    entries_by_filename = {e["filename"]: e for e in mapped_entries}

    results: List[Dict] = []

    attachable = [f for f in invoice_lookup]
    for i, filename in enumerate(attachable):
        if progress_callback:
            progress_callback(i, len(attachable), filename)

        mapping_row = invoice_lookup[filename]
        entry = entries_by_filename[filename]
        invoice_id = mapping_row["invoice_id"]

        base = {
            "filename": filename,
            "customer_id": mapping_row["customer_id"],
            "client_id": mapping_row["client_id"],
            "match_type": mapping_row["match_type"],
            "invoice_id": invoice_id,
        }

        try:
            if entry.get("match_type") == "fuzzy" and client_id_field_id:
                existing = client.get_customer_custom_field_value(
                    entry["customer_id"], field_id=client_id_field_id
                )
                if not existing:
                    client.set_customer_custom_field(
                        entry["customer_id"], client_id_field_id, str(entry["client_id"])
                    )
                    base["client_id_set"] = True

            client.put_attachment(invoice_id, entry["df"], filename)
            results.append({**base, "status": "attached", "message": "Success"})

        except Exception as e:
            results.append({**base, "status": "error", "message": str(e)})

    return results
