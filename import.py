import os
import re
import json
import pandas as pd
import psycopg2
from decimal import Decimal, InvalidOperation

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "lending_db",
    "user": "postgres",
    "password": "1"
}

DATA_FOLDER = "data"

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

def normalize_column_name(col: str) -> str:
    if col is None:
        return ""
    return str(col).strip()

def clean_money(value):
    if pd.isna(value) or value == "":
        return None
    text = str(value).strip()
    text = text.replace("$", "").replace(",", "")
    text = re.sub(r"\((.+)\)", r"-\1", text)
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None

def clean_date(value):
    if pd.isna(value) or value == "":
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()

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
    return df

def insert_raw_rows(cursor, source_file: str, df: pd.DataFrame):
    for idx, row in df.iterrows():
        row_dict = {}
        for k, v in row.to_dict().items():
            if pd.isna(v):
                row_dict[str(k)] = None
            else:
                row_dict[str(k)] = str(v)
        cursor.execute(
            """
            INSERT INTO raw_lending_activity (source_file, row_number, data)
            VALUES (%s, %s, %s::jsonb)
            """,
            (source_file, idx + 1, json.dumps(row_dict, ensure_ascii=False))
        )

def insert_clean_rows(cursor, source_file: str, df: pd.DataFrame):
    success = 0
    failed = 0

    for _, row in df.iterrows():
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
                    clean_text(row.get("matter_no")),
                    clean_text(row.get("broker")),
                    clean_text(row.get("lender")),
                    clean_text(row.get("suburb_state")),
                    clean_text(row.get("priority_level")),
                    clean_money(row.get("principal_amount")),
                    clean_money(row.get("rate")),
                    clean_money(row.get("estab_inclusive")),
                    clean_money(row.get("estab_amount")),
                    clean_money(row.get("lvr")),
                    clean_text(row.get("security_type")),
                    clean_text(row.get("partner_name")),
                    clean_text(row.get("associate_name")),
                    clean_text(row.get("scenario")),
                    clean_text(row.get("status")),
                    clean_date(row.get("settlement_date")),
                    clean_date(row.get("repayment_date")),
                    clean_text(row.get("discharged")),
                    clean_money(row.get("broker_earned")),
                    clean_text(row.get("review_of_broker")),
                    clean_money(row.get("lender_earned")),
                    clean_text(row.get("review_of_lender")),
                    clean_money(row.get("solicitor_earned_from_broker")),
                    clean_text(row.get("review_of_solicitor_by_broker")),
                    clean_money(row.get("solicitor_earned_from_lender")),
                    clean_text(row.get("review_of_solicitor_by_lender")),
                    clean_money(row.get("shortfall_amount")),
                )
            )
            success += 1
        except Exception as e:
            failed += 1
            print(f"[FAILED ROW] {source_file}: {e}")

    return success, failed

def process_excel_file(cursor, filepath: str):
    filename = os.path.basename(filepath)
    print(f"Processing: {filename}")

    excel_file = pd.ExcelFile(filepath)
    total_rows = 0
    total_success = 0
    total_failed = 0

    for sheet_name in excel_file.sheet_names:
        try:
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            print("列名：", df.columns.tolist())
            if df.empty:
                continue

            total_rows += len(df)

            insert_raw_rows(cursor, filename, df)

            df = rename_columns(df)
            df = ensure_target_columns(df)

            success, failed = insert_clean_rows(cursor, filename, df)
            total_success += success
            total_failed += failed

        except Exception as e:
            print(f"[FAILED SHEET] {filename} - {sheet_name}: {e}")

    cursor.execute(
        """
        INSERT INTO import_log (source_file, total_rows, success_rows, failed_rows)
        VALUES (%s, %s, %s, %s)
        """,
        (filename, total_rows, total_success, total_failed)
    )

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        for file in os.listdir(DATA_FOLDER):
            if file.lower().endswith(".xlsx"):
                filepath = os.path.join(DATA_FOLDER, file)
                process_excel_file(cursor, filepath)
                conn.commit()

        print("Import completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"[FATAL ERROR] {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()