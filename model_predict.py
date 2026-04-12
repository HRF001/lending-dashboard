import pickle
import numpy as np
import pandas as pd

MODEL_PATH = "broker_risk_model.pkl"

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["repayment_date"] = pd.to_datetime(df["repayment_date"], errors="coerce")
    df["discharged"] = pd.to_datetime(df["discharged"], errors="coerce")

    df["loan_term"] = (df["repayment_date"] - df["settlement_date"]).dt.days
    df["log_principal"] = np.log(df["principal_amount"].fillna(0) + 1)

    if "priority_level" in df.columns and df["priority_level"].dtype == object:
        priority_map = {
            "First": 1,
            "Second": 2,
            "Third": 3,
            "Fourth": 4
        }
        df["priority_level"] = df["priority_level"].map(priority_map)

    return df


def score_loans(df: pd.DataFrame) -> pd.DataFrame:
    df = prepare_features(df)

    features = ["priority_level", "rate", "lvr", "loan_term", "log_principal"]

    X = df[features].copy()
    df["pred_prob"] = model.predict_proba(X)[:, 1]

    return df