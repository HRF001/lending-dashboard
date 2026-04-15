import pickle
import pandas as pd
from pathlib import Path

from feature_utils import prepare_loan_features

MODEL_PATH = Path(__file__).with_name("broker_risk_model.pkl")
_model = None


def get_model():
    global _model
    if _model is None:
        with open(MODEL_PATH, "rb") as f:
            _model = pickle.load(f)
    return _model


def score_loans(df: pd.DataFrame) -> pd.DataFrame:
    df = prepare_loan_features(df)

    features = ["priority_level", "rate", "lvr", "loan_term", "log_principal"]

    X = df[features].copy()
    df["pred_prob"] = get_model().predict_proba(X)[:, 1]

    return df
