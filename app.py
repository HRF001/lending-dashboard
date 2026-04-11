from flask import Flask, render_template, jsonify
import psycopg2
import subprocess

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host=os.environ["dpg-d7cvcndckfvc73efcubg-a.oregon-postgres.render.com"],
        port=os.environ.get("PGPORT", 5432),
        dbname=os.environ["omicron"],
        user=os.environ["omicron_user"],
        password=os.environ["pxYOIUbbg1nd93565IONMBG6Dvc4niQE"]
    )

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
    cur = conn.cursor()

    try:
        cur.execute("""
        WITH base AS (
            SELECT
                broker,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal,
                AVG(lvr) AS avg_lvr,
                AVG(rate) AS avg_rate
            FROM clean_lending_activity
            WHERE broker IS NOT NULL
              AND broker NOT ILIKE '%not disclosed%'
              AND broker NOT ILIKE '%no broker%'
              AND broker NOT ILIKE '%direct%'
            GROUP BY broker
        ),
        norm AS (
            SELECT
                broker,
                deals,
                total_principal,
                avg_lvr,
                avg_rate,
                LOG(total_principal + 1) / NULLIF(MAX(LOG(total_principal + 1)) OVER(), 0) AS n_principal,
                deals * 1.0 / NULLIF(MAX(deals) OVER(), 0) AS n_deals,
                avg_lvr / NULLIF(MAX(avg_lvr) OVER(), 0) AS n_lvr,
                avg_rate / NULLIF(MAX(avg_rate) OVER(), 0) AS n_rate
            FROM base
        )
        SELECT
            broker,
            deals,
            total_principal,
            avg_lvr,
            avg_rate,
            (
                0.40 * COALESCE(n_principal, 0) +
                0.30 * COALESCE(n_deals, 0) -
                0.20 * COALESCE(n_lvr, 0) -
                0.10 * COALESCE(n_rate, 0)
            ) AS score
        FROM norm
        ORDER BY score DESC
        LIMIT 20;
        """)

        rows = cur.fetchall()

        return jsonify([
            {
                "broker": r[0],
                "deals": int(r[1] or 0),
                "principal": float(r[2] or 0),
                "lvr": float(r[3] or 0),
                "rate": float(r[4] or 0),
                "score": float(r[5] or 0)
            }
            for r in rows
        ])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
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
                settlement_date,
                COUNT(*) AS deals,
                SUM(principal_amount) AS total_principal
            FROM clean_lending_activity
            WHERE settlement_date IS NOT NULL
            GROUP BY settlement_date
            ORDER BY settlement_date
        """)

        rows = cur.fetchall()

        return jsonify([
            {
                "date": str(r[0]),
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
