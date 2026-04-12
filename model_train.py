import os
import pickle
from typing import Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


MODEL_PATH = "loan_risk_model.pkl"


def get_conn():
    return psycopg2.connect(
        host="dpg-d7cvcndckfvc73efcubg-a.oregon-postgres.render.com",
        port=5432,
        dbname="omicron",
        user="omicron_user",
        password="pxYOIUbbg1nd93565IONMBG6Dvc4niQE"
    )


def load_training_data() -> pd.DataFrame:
    """
    从数据库加载训练数据。
    这里默认你的表已经有这些字段：
    - principal_amount
    - lvr
    - rate
    - settlement_date
    - repayment_date
    - discharged

    如果你库里还是原始 Excel 名字，就把 SQL 里的列名改成你的真实字段名。
    """
    sql = """
        SELECT
            principal_amount,
            lvr,
            rate,
            settlement_date,
            repayment_date,
            discharged
        FROM clean_lending_activity
        WHERE settlement_date IS NOT NULL
          AND repayment_date IS NOT NULL
    """

    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["repayment_date"] = pd.to_datetime(df["repayment_date"], errors="coerce")
    df["discharged"] = pd.to_datetime(df["discharged"], errors="coerce")

    # 贷款期限
    # df["loan_term_days"] = (df["repayment_date"] - df["settlement_date"]).dt.days

    # 金额取 log，减少极端值影响
    df["log_principal"] = (df["principal_amount"].fillna(0) + 1).apply(lambda x: np.log(x))

    # 逾期标签：
    # 已经过了 repayment_date，且还没 discharged，记为 1
    today = pd.Timestamp.today().normalize()
    df["overdue_flag"] = (
        df["discharged"].isna() &
        (df["repayment_date"] < today)
    ).astype(int)

    return df


def prepare_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    feature_cols = ["lvr", "rate", "log_principal"]
    target_col = "overdue_flag"

    model_df = df[feature_cols + [target_col]].copy()

    # 去掉完全无效的行
    model_df = model_df.dropna(subset=[target_col])

    X = model_df[feature_cols]
    y = model_df[target_col]

    return X, y


def train_model(X: pd.DataFrame, y: pd.Series) -> Pipeline:
    numeric_features = ["lvr", "rate", "log_principal"]

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
        ]
    )

    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=42,
    )

    clf = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("model", model),
    ])

    clf.fit(X, y)
    return clf


def evaluate_model(model: Pipeline, X_test: pd.DataFrame, y_test: pd.Series) -> None:
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("\n=== Classification Report ===")
    print(classification_report(y_test, y_pred, digits=4))

    try:
        auc = roc_auc_score(y_test, y_prob)
        print(f"AUC: {auc:.4f}")
    except ValueError:
        print("AUC 无法计算，可能测试集只有一个类别。")


def print_feature_weights(model: Pipeline) -> None:
    """
    打印 Logistic Regression 的系数，作为“数据学出来的权重参考”。
    注意：这是标准化后的系数，适合比较相对重要性。
    """
    feature_names = ["lvr", "rate", "log_principal"]
    lr = model.named_steps["model"]
    coefs = lr.coef_[0]

    coef_df = pd.DataFrame({
        "feature": feature_names,
        "coefficient": coefs,
        "abs_coefficient": abs(coefs),
    }).sort_values("abs_coefficient", ascending=False)

    print("\n=== Learned Coefficients ===")
    print(coef_df[["feature", "coefficient"]].to_string(index=False))

    coef_df["weight_pct"] = coef_df["abs_coefficient"] / coef_df["abs_coefficient"].sum()
    print("\n=== Relative Weight Approximation ===")
    print(coef_df[["feature", "weight_pct"]].to_string(index=False))


def save_model(model: Pipeline, path: str = MODEL_PATH) -> None:
    with open(path, "wb") as f:
        pickle.dump(model, f)


def main():
    df = load_training_data()
    df = build_features(df)

    X, y = prepare_xy(df)

    if y.nunique() < 2:
        raise ValueError("当前 overdue_flag 只有一个类别，无法训练分类模型。请先检查数据。")

    print(f"样本数: {len(X)}")
    print(f"逾期比例: {y.mean():.4f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    model = train_model(X_train, y_train)
    evaluate_model(model, X_test, y_test)
    print_feature_weights(model)
    save_model(model)

    print(f"\n模型已保存到: {os.path.abspath(MODEL_PATH)}")


if __name__ == "__main__":
    main()