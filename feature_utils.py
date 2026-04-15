import numpy as np
import pandas as pd


PRIORITY_MAP = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
}


def normalize_priority_level(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(PRIORITY_MAP)
    )


def normalize_status(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
    )


def compute_closed_flag(df: pd.DataFrame) -> pd.Series:
    normalized_status = normalize_status(df["status"]) if "status" in df.columns else pd.Series("", index=df.index)
    discharged_dates = pd.to_datetime(df["discharged"], errors="coerce") if "discharged" in df.columns else pd.Series(pd.NaT, index=df.index)
    return normalized_status.eq("discharged") | discharged_dates.notna()


def compute_overdue_flag(df: pd.DataFrame, today=None) -> pd.Series:
    repayment_dates = pd.to_datetime(df["repayment_date"], errors="coerce")
    today = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()
    return ((~compute_closed_flag(df)) & (repayment_dates < today)).astype(int)


def prepare_loan_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["repayment_date"] = pd.to_datetime(df["repayment_date"], errors="coerce")
    df["discharged"] = pd.to_datetime(df["discharged"], errors="coerce")

    df["loan_term"] = (df["repayment_date"] - df["settlement_date"]).dt.days
    df["log_principal"] = np.log(df["principal_amount"].fillna(0) + 1)

    if "priority_level" in df.columns:
        df["priority_level"] = normalize_priority_level(df["priority_level"])

    return df
