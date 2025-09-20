from typing import List


def apply_summary_formatting(sheet_id):
    def format_section(bg_color, start_row, stop_row):
        red, green, blue = [color / 255 if color > 1 else color for color in bg_color]
        return [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': start_row - 1,
                        'endRowIndex': stop_row,
                        'startColumnIndex': 0,
                        'endColumnIndex': 2
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {'red': red, 'green': green, 'blue': blue}
                        }
                    },
                    'fields': 'userEnteredFormat.backgroundColor'
                }
            }
        ]

    """ Old sections, can be reinstated once all data is up to date
    sections = [
        ([182, 215, 168], 1, 1),
        ([217, 217, 217], 3, 3),
        ([217, 210, 233], 4, 5),
        ([201, 218, 248], 6, 9),
        ([239, 239, 239], 10, 12),
        ([252, 229, 205], 13, 14),
        ([208, 224, 227], 15, 18),
        ([230, 184, 175], 19, 20),
        ([217, 210, 233], 21, 24)
    ]"""
    sections = [
        ([182, 215, 168], 1, 1),
        ([217, 217, 217], 3, 3),
        ([183, 224, 205], 4, 9),    # Green color (updated insights)
        ([248, 243, 201], 10, 25)   # Yellow color (not updated yet)
    ]

    format_requests = []
    for color, start, end in sections:
        format_requests.extend(format_section(color, start, end))

    # Add column formatting
    format_requests.extend([
        # Column 1 formatting: bold and size 13
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startColumnIndex': 0,
                    'endColumnIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'bold': True,
                            'fontSize': 13
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat(bold,fontSize)'
            }
        },
        # Column 2 formatting: size 11, not bold
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startColumnIndex': 1,
                    'endColumnIndex': 2
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontSize': 11
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat.fontSize'
            }
        },
        # Set column 1 width to 400px
        {
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': 1
                },
                'properties': {
                    'pixelSize': 400
                },
                'fields': 'pixelSize'
            }
        },
        # Set column 2 width to 750px
        {
            'updateDimensionProperties': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 1,
                    'endIndex': 2
                },
                'properties': {
                    'pixelSize': 750
                },
                'fields': 'pixelSize'
            }
        },
        # Set cell A3 to size 16 font
        {
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'endRowIndex': 3,
                    'startColumnIndex': 0,
                    'endColumnIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {
                            'fontSize': 16
                        }
                    }
                },
                'fields': 'userEnteredFormat.textFormat.fontSize'
            }
        }
    ])

    return format_requests


def pack_wr_by_asc_formatting(content: dict, sheet_id: int) -> list:
    format_requests = [{
        'addConditionalFormatRule': {
            'rule': {
                'ranges': [{
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'startColumnIndex': 2,
                    'endColumnIndex': len(content)
                }],
                'booleanRule': {
                    'condition': {
                        'type': 'CUSTOM_FORMULA',
                        'values': [{
                            'userEnteredValue': '=AND(C3<$B3, C3<>"N/A")'
                        }]
                    },
                    'format': {
                        'backgroundColor': {
                            'red': 0.957,
                            'green': 0.8,
                            'blue': 0.8
                        }
                    }
                }
            },
            'index': 0
        }
    }, {
        'addConditionalFormatRule': {
            'rule': {
                'ranges': [{
                    'sheetId': sheet_id,
                    'startRowIndex': 2,
                    'startColumnIndex': 2,
                    'endColumnIndex': len(content)
                }],
                'booleanRule': {
                    'condition': {
                        'type': 'CUSTOM_FORMULA',
                        'values': [{
                            'userEnteredValue': '=AND(C3>$B3, C3<>"N/A")'
                        }]
                    },
                    'format': {
                        'backgroundColor': {
                            'red': 0.718,
                            'green': 0.882,
                            'blue': 0.804
                        }
                    }
                }
            },
            'index': 1
        }
    }, {
        'updateDimensionProperties': {
            'range': {
                'sheetId': sheet_id,
                'dimension': 'COLUMNS',
                'startIndex': 2,
                'endIndex': len(content)
            },
            'properties': {
                'pixelSize': 52
            },
            'fields': 'pixelSize'
        }
    }, {
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': 1,
                'startColumnIndex': 1,
                'endColumnIndex': len(content)
            },
            'cell': {
                'userEnteredFormat': {
                    'horizontalAlignment': 'CENTER'
                }
            },
            'fields': 'userEnteredFormat.horizontalAlignment'
        }
    }]

    # Conditional Formatting Rules

    # Column Resizing

    return format_requests


def freeze_rows_request(sheet_id: int, *, frozen: int = 1) -> dict:
    return {
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": frozen}},
            "fields": "gridProperties.frozenRowCount",
        }
    }


def header_format_request(sheet_id: int, *, header_row_index: int, end_col: int) -> dict:
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": header_row_index,
                "endRowIndex": header_row_index + 1,
                "startColumnIndex": 0,
                "endColumnIndex": end_col,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 1},
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "wrapStrategy": "WRAP",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)",
        }
    }


def auto_resize_request(sheet_id: int, *, end_col: int) -> dict:
    return {
        "autoResizeDimensions": {
            "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": end_col}
        }
    }


def basic_filter_request(sheet_id: int, headers: List[str], *, start_row: int, start_col: int, end_col: int) -> dict:
    if "Pack" in headers:
        pack_col = headers.index("Pack")
        return {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                    "filterSpecs": [{
                        "filterCriteria": {
                            "condition": {"type": "TEXT_NOT_CONTAINS", "values": [{"userEnteredValue": ":"}]}
                        },
                        "columnIndex": pack_col,
                    }],
                }
            }
        }
    return {
        "setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col,
                }
            }
        }
    }
