from flask import Flask, render_template, jsonify
import psycopg2
import subprocess
import os
import pandas as pd
from model_predict import score_loans
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"]
    )
"""
def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="lending_db",     # 改成你的
        user="postgres",       # 改成你的
        password="1"      # 改成你的
    )
"""    
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/overview")
def overview():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                COUNT(*) AS total_deals,
                SUM(principal_amount) AS total_principal,
                AVG(principal_amount) AS avg_principal
            FROM clean_lending_activity
        """)

        row = cur.fetchone()

        return jsonify({
            "total_deals": int(row[0] or 0),
            "total_principal": float(row[1] or 0),
            "avg_principal": float(row[2] or 0)
        })
    finally:
        cur.close()
        conn.close()

@app.route("/api/top-brokers")
def top_brokers():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                broker,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal
            FROM clean_lending_activity
            WHERE broker IS NOT NULL
              AND broker NOT ILIKE '%not disclosed%'
              AND broker NOT ILIKE '%no broker%'
              AND broker NOT ILIKE '%direct%'
            GROUP BY broker
            ORDER BY SUM(principal_amount) DESC
            LIMIT 10
        """)

        rows = cur.fetchall()

        return jsonify([
            {
                "broker": r[0],
                "deals": int(r[1] or 0),
                "total": float(r[2] or 0)
            }
            for r in rows
        ])
    finally:
        cur.close()
        conn.close()

@app.route("/api/broker-risk")
def broker_risk():
    conn = get_conn()

    try:
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
              AND broker NOT ILIKE '%not disclosed%'
              AND broker NOT ILIKE '%no broker%'
              AND broker NOT ILIKE '%direct%'
              AND settlement_date IS NOT NULL
              AND repayment_date IS NOT NULL
              AND lvr > 0
        """

        df = pd.read_sql(query, conn)

        if df.empty:
            return jsonify([])

        # 模型打分：每笔贷款预测逾期概率
        df = score_loans(df)

        # 真实逾期标签
        df["repayment_date"] = pd.to_datetime(df["repayment_date"], errors="coerce")
        df["discharged"] = pd.to_datetime(df["discharged"], errors="coerce")

        today = pd.Timestamp.today().normalize()
        df["overdue_flag"] = (
            df["discharged"].isna() &
            (df["repayment_date"] < today)
        ).astype(int)

        # 聚合成 broker 风险
        broker_df = (
            df.groupby("broker")
            .agg(
                deals=("broker", "size"),
                principal=("principal_amount", "sum"),
                lvr=("lvr", "mean"),
                rate=("rate", "mean"),
                overdue_rate=("overdue_flag", "mean"),
                score=("pred_prob", "mean")
            )
            .reset_index()
        )

        # 转百分制
        broker_df["score"] = (broker_df["score"] * 100).round(1)
        broker_df["overdue_rate"] = (broker_df["overdue_rate"] * 100).round(1)

        # 风险等级
        def risk_level(x):
            if x >= 75:
                return "High Risk"
            elif x >= 50:
                return "Elevated Risk"
            elif x >= 25:
                return "Moderate Risk"
            return "Low Risk"

        broker_df["grade"] = broker_df["score"].apply(risk_level)

        broker_df = broker_df.sort_values("score", ascending=False)

        return jsonify([
            {
                "broker": row["broker"],
                "deals": int(row["deals"]) if pd.notna(row["deals"]) else 0,
                "principal": float(row["principal"]) if pd.notna(row["principal"]) else 0,
                "lvr": float(row["lvr"]) if pd.notna(row["lvr"]) else 0,
                "rate": float(row["rate"]) if pd.notna(row["rate"]) else 0,
                "overdue_rate": float(row["overdue_rate"]) if pd.notna(row["overdue_rate"]) else 0,
                "score": float(row["score"]) if pd.notna(row["score"]) else 0,
                "grade": row["grade"]
            }
            for _, row in broker_df.iterrows()
        ])

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()

def top_lenders():
        conn = get_conn()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT
                    lender,
                    COUNT(*) AS deals,
                    SUM(principal_amount) AS total_principal
                FROM clean_lending_activity
                WHERE lender IS NOT NULL
                AND lender <> 'Privacy Settings Engaged'
                AND TRIM(lender) <> ''
                GROUP BY lender
                ORDER BY total_principal DESC
                LIMIT 10
            """)
            rows = cur.fetchall()

            results = []
            for row in rows:
                results.append({
                    "lender": row[0],
                    "deals": row[1],
                    "principal": float(row[2]) if row[2] is not None else 0
                })
            return results

        finally:
            cur.close()
            conn.close()

@app.route("/api/lender-aggressiveness")
def lender_aggressiveness():
    conn = get_conn()

    try:
        query = """
            SELECT
                lender,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal,
                AVG(lvr) AS avg_lvr,
                AVG(rate) AS avg_rate,
                AVG(
                    EXTRACT(DAY FROM (repayment_date::timestamp - settlement_date::timestamp))
                ) AS avg_term,
                AVG(
                    CASE
                        WHEN LOWER(TRIM(priority_level)) = 'second' THEN 1.0
                        ELSE 0.0
                    END
                ) AS second_share
            FROM clean_lending_activity
            WHERE lender IS NOT NULL
              AND TRIM(lender) <> ''
              AND lvr > 0
              AND settlement_date IS NOT NULL
              AND repayment_date IS NOT NULL
              AND lender NOT ILIKE '%privacy%'
              AND lender NOT ILIKE '%investor owned by broker%'
            GROUP BY lender
            HAVING COUNT(*) >= 5
        """

        df = pd.read_sql(query, conn)

        if df.empty:
            return jsonify([])

        feature_cols = ["avg_lvr", "avg_rate", "avg_term", "second_share"]
        X = df[feature_cols].fillna(0).copy()

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df["cluster"] = kmeans.fit_predict(X_scaled)

        centers = pd.DataFrame(
            scaler.inverse_transform(kmeans.cluster_centers_),
            columns=feature_cols
        )
        centers["cluster"] = range(3)

        # 用聚类中心给 cluster 贴标签
        centers["aggressiveness_proxy"] = (
            0.35 * centers["avg_lvr"] +
            0.25 * centers["avg_rate"] +
            0.20 * centers["second_share"] +
            0.20 * (1 / (centers["avg_term"] + 1))
        )

        centers = centers.sort_values("aggressiveness_proxy").reset_index(drop=True)

        label_map = {
            int(centers.iloc[0]["cluster"]): "Conservative",
            int(centers.iloc[1]["cluster"]): "Balanced",
            int(centers.iloc[2]["cluster"]): "Aggressive",
        }

        df["level"] = df["cluster"].map(label_map)

        # 连续分数：每个 lender 自己算，不再用 cluster 统一打分
        df["raw_score"] = (
            0.35 * df["avg_lvr"] +
            0.25 * df["avg_rate"] +
            0.20 * df["second_share"] +
            0.20 * (1 / (df["avg_term"] + 1))
        )

        if df["raw_score"].max() > df["raw_score"].min():
            df["score"] = (
                (df["raw_score"] - df["raw_score"].min()) /
                (df["raw_score"].max() - df["raw_score"].min()) * 100
            ).round(1)
        else:
            df["score"] = 50.0

        df = df.sort_values(["score", "avg_lvr", "avg_rate"], ascending=False)

        return jsonify([
            {
                "lender": row["lender"],
                "deals": int(row["deals"]) if pd.notna(row["deals"]) else 0,
                "principal": float(row["total_principal"]) if pd.notna(row["total_principal"]) else 0,
                "lvr": float(row["avg_lvr"]) if pd.notna(row["avg_lvr"]) else 0,
                "rate": float(row["avg_rate"]) if pd.notna(row["avg_rate"]) else 0,
                "term": float(row["avg_term"]) if pd.notna(row["avg_term"]) else 0,
                "second_share": float(row["second_share"] * 100) if pd.notna(row["second_share"]) else 0,
                "score": float(row["score"]) if pd.notna(row["score"]) else 0,
                "grade": row["level"]
            }
            for _, row in df.iterrows()
        ])

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()
        
@app.route("/api/top-lenders")
def api_top_lenders():
    return jsonify(top_lenders())


def partner_score_analysis(conn):
    sql = """
    WITH partner_base AS (
        SELECT
            partner_name,
            COUNT(*) AS deals,
            AVG(
                CASE
                    WHEN discharged IS NULL
                     AND repayment_date < CURRENT_DATE
                    THEN 1.0 ELSE 0.0
                END
            ) AS overdue_rate
        FROM clean_lending_activity
        WHERE partner_name IS NOT NULL
          AND partner_name <> ''
        GROUP BY partner_name
        HAVING COUNT(*) >= 20
    )
    SELECT
        partner_name,
        deals,
        ROUND(overdue_rate::numeric, 4) AS overdue_rate,
        ROUND(((1 - overdue_rate) * 100)::numeric, 2) AS score,
        CASE
            WHEN (1 - overdue_rate) * 100 >= 80 THEN 'A'
            WHEN (1 - overdue_rate) * 100 >= 65 THEN 'B'
            WHEN (1 - overdue_rate) * 100 >= 50 THEN 'C'
            ELSE 'D'
        END AS grade
    FROM partner_base
    ORDER BY overdue_rate ASC, deals ASC
    """
    return pd.read_sql(sql, conn)

@app.route("/api/partner-risk")
def api_partner_risk():
    conn = get_conn()
    try:
        df = partner_score_analysis(conn)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        print("Partner risk error:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/market-structure")
def market_structure():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                CASE
                    WHEN broker IS NULL THEN 'UNKNOWN'
                    WHEN broker ILIKE '%not disclosed%' THEN 'UNKNOWN'
                    WHEN broker ILIKE '%no broker%' THEN 'DIRECT'
                    WHEN broker ILIKE '%direct%' THEN 'DIRECT'
                    ELSE 'BROKER'
                END AS broker_type,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal
            FROM clean_lending_activity
            GROUP BY broker_type
            ORDER BY total_principal DESC
        """)

        rows = cur.fetchall()

        return jsonify([
            {
                "type": r[0],
                "deals": int(r[1] or 0),
                "principal": float(r[2] or 0)
            }
            for r in rows
        ])
    finally:
        cur.close()
        conn.close()

@app.route("/api/trend")
def trend():
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                TO_CHAR(DATE_TRUNC('month', settlement_date), 'YYYY-MM') AS month,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal
            FROM clean_lending_activity
            WHERE settlement_date IS NOT NULL
            GROUP BY DATE_TRUNC('month', settlement_date)
            ORDER BY DATE_TRUNC('month', settlement_date)
        """)

        rows = cur.fetchall()

        return jsonify([
            {
                "date": r[0],
                "deals": int(r[1] or 0),
                "principal": float(r[2] or 0)
            }
            for r in rows
        ])
    finally:
        cur.close()
        conn.close()
        
@app.route("/api/refresh")
def refresh():
    try:
        subprocess.run(["python", "import_excels.py"], check=True)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
