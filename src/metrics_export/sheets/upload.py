import datetime
from pathlib import Path
from typing import Dict, List, Any

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from metrics_export.sheets.formatting import apply_summary_formatting, pack_wr_by_asc_formatting, freeze_rows_request, header_format_request, \
    auto_resize_request, basic_filter_request

# Full access scope allows for reading and writing.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = "146GPNf1aCHj5URk_oMkYS064HuRcP4vgbtCAkQ9NVWo"

token_path = Path("data/sheets/token.json")
credentials_path = Path("data/sheets/credentials.json")


# --- Auth ---
def auth():
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                # Token revoked/invalid -> force new login
                creds = None

        # If no valid creds, run local server flow to get new token
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
            # offline + consent ensures a refresh token is issued
            creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

        # Save the credentials for the next run
        token_path.write_text(creds.to_json())

    service = build("sheets", "v4", credentials=creds)
    return service.spreadsheets()


# --- Low-level helpers ---
def get_sheets_map(sheet) -> Dict[str, int]:
    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}


def get_sheets_list(sheet) -> List[dict]:
    meta = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
    return meta.get("sheets", [])


def ensure_sheet(sheet, title: str, rows: int, cols: int, *, index: int | None = None) -> int:
    """Create if missing. Return sheetId."""
    existing = get_sheets_map(sheet)
    if title in existing:
        return existing[title]

    req = [{
        "addSheet": {
            "properties": {
                "title": title,
                **({"index": index} if index is not None else {}),
                "gridProperties": {"rowCount": rows, "columnCount": cols}
            }
        }
    }]
    resp = sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": req}).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def write_values(sheet, title: str, values: List[List[Any]], start_cell: str = "A1") -> None:
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{title}!{start_cell}",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def apply_requests(sheet, requests: List[dict]) -> None:
    if requests:
        sheet.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()


# --- Public API ---
def update_insights(insights: Dict[str, dict]) -> None:
    """
    insights = {
      "SheetName": {
         "description": str,
         "headers": [],
         "data": [[]]
      },
      ...
    }
    """
    sheet = auth()

    try:
        for title, content in insights.items():
            headers = content["headers"]
            rows = len(content["data"]) + 2
            cols = len(headers) + 1  # leave extra col for the long description

            # Always drop+recreate to ensure formatting is reset
            sheet_id = replace_sheet(title, rows=rows, cols=cols)

            # Build values: description, headers, data
            values = [[content["description"]], headers, *content["data"]]
            write_values(sheet, title, values)

            # Formatting requests
            reqs = [
                freeze_rows_request(sheet_id, frozen=2),
                header_format_request(sheet_id, header_row_index=1, end_col=len(headers)),
                auto_resize_request(sheet_id, end_col=len(headers)),
                basic_filter_request(sheet_id, headers, start_row=1, start_col=0, end_col=len(headers)),
            ]

            if headers and headers[0] == "Pack" and "Overall Win Rate" in headers and "A20" in headers:
                reqs.extend(pack_wr_by_asc_formatting(headers, sheet_id))

            apply_requests(sheet, reqs)

    except HttpError as err:
        raise RuntimeError(f"update_insights failed: {err}")


def update_summary_sheet() -> None:
    sheet = auth()

    try:
        sheets = get_sheets_list(sheet)
        title_to_id = {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}

        if "Summary" not in title_to_id:
            summary_id = ensure_sheet(sheet, "Summary", rows=100, cols=4, index=0)
        else:
            summary_id = title_to_id["Summary"]

        now = datetime.datetime.now().strftime("%Y/%m/%d %H:%M")
        values: List[List[Any]] = [
            [f"Last updated: {now}"],
            [],
            ["Quick navigation"],
        ]

        for t, sid in title_to_id.items():
            if t == "Summary":
                continue
            # Safely read description cell
            desc_resp = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID, range=f"{t}!A1"
            ).execute()
            description = desc_resp.get("values", [[""]])[0][0]
            link = f'=HYPERLINK("#gid={sid}", "{t}")'
            values.append([link, description])

        write_values(sheet, "Summary", values)

        # Formatting centralized in formatting.py
        apply_requests(sheet, apply_summary_formatting(summary_id))

    except HttpError as err:
        raise RuntimeError(f"update_summary_sheet failed: {err}")


def delete_all_sheets_except_first(spreadsheet_id: str = SPREADSHEET_ID) -> int:
    """Deletes all sheets except the first tab. Returns count."""
    sheet = auth()
    meta = sheet.get(spreadsheetId=spreadsheet_id).execute()
    tabs = meta.get("sheets", [])
    delete_reqs = [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}} for s in tabs[1:]]
    if delete_reqs:
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": delete_reqs}).execute()
    return len(delete_reqs)


def replace_sheet(title: str, *, spreadsheet_id: str = SPREADSHEET_ID, rows: int = 100, cols: int = 26) -> int:
    """
    Delete a sheet by title and re-create it in the same position.
    Returns new sheetId.
    """
    sheet = auth()
    meta = sheet.get(spreadsheetId=spreadsheet_id).execute()
    target = next((s for s in meta.get("sheets", []) if s["properties"]["title"] == title), None)

    # If missing, just add
    if not target:
        resp = sheet.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{
                "addSheet": {"properties": {"title": title, "gridProperties": {"rowCount": rows, "columnCount": cols}}}
            }]}
        ).execute()
        return resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    sheet_id = target["properties"]["sheetId"]
    index = target["properties"].get("index", 0)

    # Atomic delete + add at original index
    reqs = [
        {"deleteSheet": {"sheetId": sheet_id}},
        {"addSheet": {
            "properties": {
                "title": title,
                "index": index,
                "gridProperties": {"rowCount": rows, "columnCount": cols}
            }
        }},
    ]
    resp = sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
    return resp["replies"][1]["addSheet"]["properties"]["sheetId"]
