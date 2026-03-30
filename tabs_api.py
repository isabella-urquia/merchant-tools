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
        """Fetch invoices with optional filters (mirrors nos get_invoices / bridge-alpha get_invoices_by_filter)."""
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


def bulk_attach_billing_to_invoices(
    client: TabsClient,
    mapped_entries: List[Dict],
    issue_date: str,
    client_id_field_id: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> List[Dict]:
    """
    Bulk-attach billing CSV files to their corresponding Tabs invoices.

    For entries that were fuzzy-matched (``match_type == "fuzzy"``), the
    Client ID custom field is set on the customer before attaching — if
    *client_id_field_id* is provided.

    Args:
        client: Authenticated TabsClient instance.
        mapped_entries: Output of dosespot_processing.map_billing_files_to_customers.
        issue_date: Invoice issue date (YYYY-MM-DD) used to locate the target invoice.
        client_id_field_id: The manufacturerCustomFieldId for the "Client ID" custom
                            field.  When provided, fuzzy-matched entries will have the
                            field set on the customer automatically.
        progress_callback: Optional (current_index, total, filename) callback.

    Returns:
        List of result dicts per entry.
    """
    results: List[Dict] = []

    for i, entry in enumerate(mapped_entries):
        if progress_callback:
            progress_callback(i, len(mapped_entries), entry["filename"])

        base = {
            "filename": entry["filename"],
            "customer_id": entry["customer_id"],
            "client_id": entry["client_id"],
            "match_type": entry.get("match_type", "client_id"),
        }

        try:
            # Backfill Client ID custom field for fuzzy-matched customers
            if entry.get("match_type") == "fuzzy" and client_id_field_id:
                existing = client.get_customer_custom_field_value(
                    entry["customer_id"], field_id=client_id_field_id
                )
                if not existing:
                    client.set_customer_custom_field(
                        entry["customer_id"], client_id_field_id, str(entry["client_id"])
                    )
                    base["client_id_set"] = True

            invoices = client.get_invoices(
                customer_id=entry["customer_id"],
                issue_date=issue_date,
            )

            if not invoices:
                results.append({**base, "invoice_id": None, "status": "skipped", "message": "No invoice found"})
                continue

            invoice_id = invoices[0].get("id")
            client.put_attachment(invoice_id, entry["df"], entry["filename"])
            results.append({**base, "invoice_id": invoice_id, "status": "attached", "message": "Success"})

        except Exception as e:
            results.append({**base, "invoice_id": None, "status": "error", "message": str(e)})

    return results
