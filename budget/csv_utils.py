import csv
import io
from decimal import Decimal, InvalidOperation
from datetime import datetime


def parse_csv(file):
    """
    Parse an uploaded CSV file.
    Returns (headers: list[str], rows: list[dict])
    """
    content = file.read()
    if isinstance(content, bytes):
        content = content.decode("utf-8-sig")  # utf-8-sig strips BOM if present

    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    rows = [dict(row) for row in reader]
    return list(headers), rows


def _parse_date(value):
    """Attempt to parse a date string into a datetime, trying common formats."""
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%dT%H:%M:%S",
        "%m-%d-%Y",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_amount(value):
    """Parse an amount string into a Decimal, stripping currency symbols and commas."""
    if value is None:
        return None
    cleaned = str(value).strip().replace(",", "").replace("$", "").replace(" ", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def apply_schema_to_transaction(transaction, schema):
    """
    Apply a file_upload_schema to a Transaction instance, populating its
    typed fields from transaction.raw_data, then saving.

    schema shape:
    {
        "schema": {
            "transaction_date": "<csv_col>" | null,
            "posted_date":      "<csv_col>" | null,
            "description":      "<csv_col>" | null,
            "description_2":    "<csv_col>" | null,
            "category":         "<csv_col>" | null,
            "amount":           "<csv_col>" | null,
        },
        "amount_column_format": "debit_is_negative" | "debit_is_positive" | null,
        "debit_column":  "<csv_col>" | null,   # only when amount_column_format is null
        "credit_column": "<csv_col>" | null,   # only when amount_column_format is null
    }
    """
    raw = transaction.raw_data
    mapping = schema.get("schema", {})
    amount_format = schema.get("amount_column_format")

    def get_raw(field_name):
        col = mapping.get(field_name)
        if col and col in raw:
            return raw[col]
        return None

    transaction.transaction_date = _parse_date(get_raw("transaction_date"))
    transaction.posted_date = _parse_date(get_raw("posted_date"))

    desc = get_raw("description")
    transaction.description = str(desc).strip() if desc else None

    desc2 = get_raw("description_2")
    transaction.description_2 = str(desc2).strip() if desc2 else None

    cat = get_raw("category")
    transaction.category = str(cat).strip() if cat else None

    if amount_format in ("debit_is_negative", "debit_is_positive"):
        raw_amount = _parse_amount(get_raw("amount"))
        if raw_amount is not None:
            if amount_format == "debit_is_negative":
                # negative value = debit/expense, stored as-is
                transaction.amount = raw_amount
            else:
                # positive value = debit/expense, flip sign so expenses are negative
                transaction.amount = -raw_amount
        else:
            transaction.amount = None
    else:
        # Split columns: debit and credit are separate
        debit_col = schema.get("debit_column")
        credit_col = schema.get("credit_column")

        debit_val = _parse_amount(raw.get(debit_col)) if debit_col and debit_col in raw else None
        credit_val = _parse_amount(raw.get(credit_col)) if credit_col and credit_col in raw else None

        if debit_val is not None and credit_val is not None:
            # Net: credits are positive, debits are negative
            transaction.amount = credit_val - debit_val
        elif debit_val is not None:
            transaction.amount = -abs(debit_val)
        elif credit_val is not None:
            transaction.amount = abs(credit_val)
        else:
            transaction.amount = None

    transaction.save()
