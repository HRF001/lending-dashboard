import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
import psycopg2

from db_config import get_db_config

DATA_FOLDER = Path("data")
QUALITY_REPORT_PATH = Path("data_quality_issues.csv")

COLUMN_MAP = {
    "Matter No.": "matter_no",
    "Attributed Omicron Broker": "broker",
    "Attributed Omicron Lender": "lender",
    "Suburb & State": "suburb_state",
    "Priority": "priority_level",
    "Principal": "principal_amount",
    "Rate": "rate",
    "Estab (inc)": "estab_inclusive",
    "Estab": "estab_amount",
    "LVR": "lvr",
    "Security type": "security_type",
    "Partner": "partner_name",
    "Associate": "associate_name",
    "Scenario": "scenario",
    "Status": "status",
    "Settlement date": "settlement_date",
    "Repayment date": "repayment_date",
    "Discharged": "discharged",
    "Broker Earned": "broker_earned",
    "Review of Broker": "review_of_broker",
    "Lender earned": "lender_earned",
    "Review of Lender": "review_of_lender",
    "Solicitors earned from Broker": "solicitor_earned_from_broker",
    "Review of Solicitor by Broker": "review_of_solicitor_by_broker",
    "Solicitors earned from Lender": "solicitor_earned_from_lender",
    "Review of Solicitor by  Lender ": "review_of_solicitor_by_lender",
    "Shortfall": "shortfall_amount",
}

TARGET_COLUMNS = [
    "matter_no",
    "broker",
    "lender",
    "suburb_state",
    "priority_level",
    "principal_amount",
    "rate",
    "estab_inclusive",
    "estab_amount",
    "lvr",
    "security_type",
    "partner_name",
    "associate_name",
    "scenario",
    "status",
    "settlement_date",
    "repayment_date",
    "discharged",
    "broker_earned",
    "review_of_broker",
    "lender_earned",
    "review_of_lender",
    "solicitor_earned_from_broker",
    "review_of_solicitor_by_broker",
    "solicitor_earned_from_lender",
    "review_of_solicitor_by_lender",
    "shortfall_amount",
]

PRIORITY_MAP = {
    "first": "First",
    "second": "Second",
    "third": "Third",
    "fourth": "Fourth",
}

MONEY_FIELDS = {
    "principal_amount",
    "estab_inclusive",
    "broker_earned",
    "lender_earned",
    "solicitor_earned_from_broker",
    "solicitor_earned_from_lender",
    "shortfall_amount",
}

PERCENT_FIELDS = {
    "rate",
    "estab_amount",
    "lvr",
}

TEXT_FIELDS = {
    "matter_no",
    "broker",
    "lender",
    "suburb_state",
    "security_type",
    "partner_name",
    "associate_name",
    "scenario",
    "status",
    "review_of_broker",
    "review_of_lender",
    "review_of_solicitor_by_broker",
    "review_of_solicitor_by_lender",
}

DATE_FIELDS = {
    "settlement_date",
    "repayment_date",
    "discharged",
}


@dataclass
class Issue:
    source_file: str
    sheet_name: str
    row_number: int
    severity: str
    field: str
    code: str
    message: str


def normalize_column_name(col: str) -> str:
    if col is None:
        return ""
    return str(col).strip()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_text(value):
    if pd.isna(value):
        return None
    text = normalize_whitespace(str(value))
    return text or None


def parse_decimal(value):
    if pd.isna(value) or value == "":
        return None

    text = normalize_whitespace(str(value))
    if not text:
        return None

    text = text.replace("$", "").replace(",", "").replace("%", "")
    text = re.sub(r"\((.+)\)", r"-\1", text)
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def clean_money(value):
    return parse_decimal(value)


def clean_percent(value):
    number = parse_decimal(value)
    if number is None:
        return None

    if abs(number) > 1:
        number = number / Decimal("100")
    return number


def clean_date(value):
    if pd.isna(value) or value == "":
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()


def clean_priority(value):
    text = clean_text(value)
    if text is None:
        return None
    return PRIORITY_MAP.get(text.lower())


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {}
    for col in df.columns:
        raw = normalize_column_name(col)
        renamed[col] = COLUMN_MAP.get(raw, raw)
    return df.rename(columns=renamed)


def ensure_target_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[TARGET_COLUMNS].copy()


def clean_row(row) -> dict:
    cleaned = {}

    for field in TARGET_COLUMNS:
        value = row.get(field)

        if field in MONEY_FIELDS:
            cleaned[field] = clean_money(value)
        elif field in PERCENT_FIELDS:
            cleaned[field] = clean_percent(value)
        elif field in DATE_FIELDS:
            cleaned[field] = clean_date(value)
        elif field == "priority_level":
            cleaned[field] = clean_priority(value)
        elif field in TEXT_FIELDS:
            cleaned[field] = clean_text(value)
        else:
            cleaned[field] = value

    return cleaned


def validate_row(cleaned: dict):
    errors = []
    warnings = []

    def add_issue(severity, field, code, message):
        issue = {
            "severity": severity,
            "field": field,
            "code": code,
            "message": message,
        }
        if severity == "error":
            errors.append(issue)
        else:
            warnings.append(issue)

    if not cleaned["matter_no"]:
        add_issue("error", "matter_no", "missing_matter_no", "Matter number is required.")

    if cleaned["principal_amount"] is None:
        add_issue("error", "principal_amount", "invalid_principal", "Principal amount is missing or invalid.")
    elif cleaned["principal_amount"] <= 0:
        add_issue("error", "principal_amount", "non_positive_principal", "Principal amount must be greater than zero.")

    if not cleaned["settlement_date"]:
        add_issue("error", "settlement_date", "missing_settlement_date", "Settlement date is required.")

    if not cleaned["repayment_date"]:
        add_issue("error", "repayment_date", "missing_repayment_date", "Repayment date is required.")

    if cleaned["priority_level"] is None:
        add_issue("warning", "priority_level", "unknown_priority", "Priority could not be mapped to First/Second/Third/Fourth.")

    if cleaned["rate"] is None:
        add_issue("warning", "rate", "missing_rate", "Rate is missing or invalid.")
    elif cleaned["rate"] <= 0:
        add_issue("warning", "rate", "non_positive_rate", "Rate should be greater than zero.")
    elif cleaned["rate"] > Decimal("1"):
        add_issue("warning", "rate", "high_rate", "Rate is above 100% after normalization.")

    if cleaned["lvr"] is None:
        add_issue("error", "lvr", "missing_lvr", "LVR is missing or invalid.")
    elif cleaned["lvr"] <= 0:
        add_issue("error", "lvr", "non_positive_lvr", "LVR should be greater than zero.")
    elif cleaned["lvr"] > Decimal("1.5"):
        add_issue("warning", "lvr", "high_lvr", "LVR is unusually high after normalization.")

    settlement_date = cleaned["settlement_date"]
    repayment_date = cleaned["repayment_date"]
    discharged_date = cleaned["discharged"]

    if settlement_date and repayment_date and repayment_date < settlement_date:
        add_issue("error", "repayment_date", "repayment_before_settlement", "Repayment date is earlier than settlement date.")

    if settlement_date and discharged_date and discharged_date < settlement_date:
        add_issue("error", "discharged", "discharged_before_settlement", "Discharged date is earlier than settlement date.")

    if cleaned["status"] and cleaned["status"].lower() == "settled" and settlement_date is None:
        add_issue("error", "status", "settled_without_settlement_date", "Settled loans must have a settlement date.")

    return errors, warnings


def record_issues(issues, issue_log, source_file: str, sheet_name: str, row_number: int):
    for issue in issues:
        issue_log.append(
            Issue(
                source_file=source_file,
                sheet_name=sheet_name,
                row_number=row_number,
                severity=issue["severity"],
                field=issue["field"],
                code=issue["code"],
                message=issue["message"],
            )
        )


def insert_raw_rows(cursor, source_file: str, df: pd.DataFrame):
    for idx, row in df.iterrows():
        row_dict = {}
        for key, value in row.to_dict().items():
            row_dict[str(key)] = None if pd.isna(value) else str(value)

        cursor.execute(
            """
            INSERT INTO raw_lending_activity (source_file, row_number, data)
            VALUES (%s, %s, %s::jsonb)
            """,
            (source_file, idx + 1, json.dumps(row_dict, ensure_ascii=False)),
        )


def insert_clean_rows(cursor, source_file: str, sheet_name: str, df: pd.DataFrame, issue_log: list[Issue]):
    inserted = 0
    skipped = 0
    warnings_count = 0
    failed = 0

    for index, row in df.iterrows():
        row_number = index + 1
        cleaned = clean_row(row)
        errors, warnings = validate_row(cleaned)

        record_issues(errors, issue_log, source_file, sheet_name, row_number)
        record_issues(warnings, issue_log, source_file, sheet_name, row_number)
        warnings_count += len(warnings)

        if errors:
            skipped += 1
            continue

        try:
            cursor.execute(
                """
                INSERT INTO clean_lending_activity (
                    source_file,
                    matter_no,
                    broker,
                    lender,
                    suburb_state,
                    priority_level,
                    principal_amount,
                    rate,
                    estab_inclusive,
                    estab_amount,
                    lvr,
                    security_type,
                    partner_name,
                    associate_name,
                    scenario,
                    status,
                    settlement_date,
                    repayment_date,
                    discharged,
                    broker_earned,
                    review_of_broker,
                    lender_earned,
                    review_of_lender,
                    solicitor_earned_from_broker,
                    review_of_solicitor_by_broker,
                    solicitor_earned_from_lender,
                    review_of_solicitor_by_lender,
                    shortfall_amount
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (source_file, matter_no, settlement_date, principal_amount)
                DO NOTHING;
                """,
                (
                    source_file,
                    cleaned["matter_no"],
                    cleaned["broker"],
                    cleaned["lender"],
                    cleaned["suburb_state"],
                    cleaned["priority_level"],
                    cleaned["principal_amount"],
                    cleaned["rate"],
                    cleaned["estab_inclusive"],
                    cleaned["estab_amount"],
                    cleaned["lvr"],
                    cleaned["security_type"],
                    cleaned["partner_name"],
                    cleaned["associate_name"],
                    cleaned["scenario"],
                    cleaned["status"],
                    cleaned["settlement_date"],
                    cleaned["repayment_date"],
                    cleaned["discharged"],
                    cleaned["broker_earned"],
                    cleaned["review_of_broker"],
                    cleaned["lender_earned"],
                    cleaned["review_of_lender"],
                    cleaned["solicitor_earned_from_broker"],
                    cleaned["review_of_solicitor_by_broker"],
                    cleaned["solicitor_earned_from_lender"],
                    cleaned["review_of_solicitor_by_lender"],
                    cleaned["shortfall_amount"],
                ),
            )
            inserted += 1
        except Exception as exc:
            failed += 1
            issue_log.append(
                Issue(
                    source_file=source_file,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    severity="error",
                    field="database",
                    code="insert_failed",
                    message=str(exc),
                )
            )

    return inserted, skipped, failed, warnings_count


def process_excel_file(cursor, filepath: Path, issue_log: list[Issue]):
    filename = filepath.name
    print(f"Processing: {filename}")

    excel_file = pd.ExcelFile(filepath)
    total_rows = 0
    total_inserted = 0
    total_skipped = 0
    total_failed = 0
    total_warnings = 0

    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            if df.empty:
                continue

            total_rows += len(df)
            insert_raw_rows(cursor, filename, df)

            df = rename_columns(df)
            df = ensure_target_columns(df)

            inserted, skipped, failed, warnings_count = insert_clean_rows(
                cursor,
                filename,
                sheet_name,
                df,
                issue_log,
            )
            total_inserted += inserted
            total_skipped += skipped
            total_failed += failed
            total_warnings += warnings_count
        except Exception as exc:
            total_failed += 1
            issue_log.append(
                Issue(
                    source_file=filename,
                    sheet_name=sheet_name,
                    row_number=0,
                    severity="error",
                    field="sheet",
                    code="sheet_failed",
                    message=str(exc),
                )
            )

    cursor.execute(
        """
        INSERT INTO import_log (source_file, total_rows, success_rows, failed_rows)
        VALUES (%s, %s, %s, %s)
        """,
        (filename, total_rows, total_inserted, total_skipped + total_failed),
    )

    print(
        f"Completed {filename}: "
        f"rows={total_rows}, inserted={total_inserted}, skipped={total_skipped}, "
        f"failed={total_failed}, warnings={total_warnings}"
    )


def write_quality_report(issue_log: list[Issue]):
    if not issue_log:
        if QUALITY_REPORT_PATH.exists():
            QUALITY_REPORT_PATH.unlink()
        print("No data quality issues found.")
        return

    report_df = pd.DataFrame([issue.__dict__ for issue in issue_log])
    report_df.to_csv(QUALITY_REPORT_PATH, index=False)
    print(f"Quality report written to {QUALITY_REPORT_PATH}")


def main():
    conn = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()
    issue_log: list[Issue] = []

    try:
        for file in sorted(DATA_FOLDER.glob("*.xlsx")):
            if file.name.startswith("~$"):
                continue
            process_excel_file(cursor, file, issue_log)
            conn.commit()

        write_quality_report(issue_log)
        print("Import completed successfully.")
    except Exception as exc:
        conn.rollback()
        print(f"[FATAL ERROR] {exc}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
