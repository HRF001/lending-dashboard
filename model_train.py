import pickle
import numpy as np
import pandas as pd
import psycopg2

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def get_conn():
    return psycopg2.connect(
        host="dpg-d7cvcndckfvc73efcubg-a.oregon-postgres.render.com",
        port=5432,
        dbname="omicron",
        user="omicron_user",
        password="pxYOIUbbg1nd93565IONMBG6Dvc4niQE"
    )


def load_data():
    query = """
        SELECT
            broker,
            priority_level,
            principal_amount,
            rate,
            lvr,
            settlement_date,
            repayment_date,
            discharged
        FROM clean_lending_activity
        WHERE broker IS NOT NULL
          AND TRIM(broker) <> ''
          AND settlement_date IS NOT NULL
          AND repayment_date IS NOT NULL
    """

    conn = get_conn()
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    return df


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    priority_map = {
        "First": 1,
        "Second": 2,
        "Third": 3,
        "Fourth": 4
    }

    df["priority_level"] = df["priority_level"].map(priority_map)

    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["repayment_date"] = pd.to_datetime(df["repayment_date"], errors="coerce")
    df["discharged"] = pd.to_datetime(df["discharged"], errors="coerce")

    # 贷款期限
    df["loan_term"] = (df["repayment_date"] - df["settlement_date"]).dt.days

    # 金额取 log，避免极端值影响
    df["log_principal"] = np.log(df["principal_amount"].fillna(0) + 1)

    # 逾期标签：到期且未 discharged
    today = pd.Timestamp.today().normalize()
    df["overdue_flag"] = (
        df["discharged"].isna() &
        (df["repayment_date"] < today)
    ).astype(int)

    # 基本过滤
    df = df[df["loan_term"].notna()]
    df = df[df["loan_term"] >= 0]

    return df


def train_model(df: pd.DataFrame):
    features = [
        "priority_level",
        "rate",
        "lvr",
        "loan_term",
        "log_principal"
    ]
    target = "overdue_flag"

    X = df[features]
    y = df[target]

    if y.nunique() < 2:
        raise ValueError("overdue_flag 只有一个类别，没法训练模型，请检查数据。")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, features)
        ]
    )

    model = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced"))
    ])

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("=== Classification Report ===")
    print(classification_report(y_test, y_pred, digits=4))

    print("=== AUC ===")
    print(round(roc_auc_score(y_test, y_prob), 4))

    # 输出系数
    clf = model.named_steps["clf"]
    coef_df = pd.DataFrame({
        "feature": features,
        "coefficient": clf.coef_[0]
    }).sort_values("coefficient", ascending=False)

    print("\n=== Feature Coefficients ===")
    print(coef_df.to_string(index=False))

    return model, features


def build_broker_risk(df: pd.DataFrame, model, features):
    df = df.copy()

    df["pred_prob"] = model.predict_proba(df[features])[:, 1]

    broker_risk = (
        df.groupby("broker")
        .agg(
            deals=("broker", "size"),
            avg_pred_risk=("pred_prob", "mean"),
            total_principal=("principal_amount", "sum"),
            avg_lvr=("lvr", "mean"),
            avg_rate=("rate", "mean"),
            overdue_rate=("overdue_flag", "mean")
        )
        .reset_index()
    )

    broker_risk["risk_score"] = (broker_risk["avg_pred_risk"] * 100).round(1)

    broker_risk["risk_level"] = pd.cut(
        broker_risk["avg_pred_risk"],
        bins=[-0.01, 0.25, 0.5, 0.75, 1.0],
        labels=["Low Risk", "Moderate Risk", "Elevated Risk", "High Risk"]
    )

    broker_risk = broker_risk.sort_values("avg_pred_risk", ascending=False)

    return broker_risk


def save_model(model, path="loan_risk_model.pkl"):
    with open(path, "wb") as f:
        pickle.dump(model, f)


def main():
    df = load_data()
    df = preprocess(df)

    print("样本数:", len(df))
    print("逾期率:", round(df["overdue_flag"].mean(), 4))

    model, features = train_model(df)
    save_model(model)

    broker_risk = build_broker_risk(df, model, features)

    print("\n=== Top 20 Broker Risk ===")
    print(
        broker_risk[
            ["broker", "deals", "risk_score", "risk_level", "overdue_rate", "avg_lvr", "avg_rate"]
        ].head(20).to_string(index=False)
    )

    broker_risk.to_csv("broker_risk_scores.csv", index=False)
    print("\n已保存：broker_risk_scores.csv")
    print("已保存模型：loan_risk_model.pkl")


if __name__ == "__main__":
    main()