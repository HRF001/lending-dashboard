## 📌 项目简介

## 🌐 在线演示
👉 https://lending-dashboard.onrender.com/

本项目通过对贷款数据进行清洗、建模和可视化，实现以下目标：

- 分析贷款市场整体情况  
- 评估 Broker 风险水平  
- 分析 Lender 的放贷风格  
- 监控律师（Partner）表现  
- 提供直观的可视化 Dashboard  

---

## 🧩 主要功能

### 1. 数据可视化 Dashboard
- 总交易数 / 总放款金额 / 平均金额  
- 市场结构（Broker / Direct）  
- 抵押类型分布  
- 月度放款趋势  
- Top Brokers / Lenders  

---

### 2. Broker 风险评分

基于机器学习模型，对每笔贷款计算违约概率，并按 Broker 聚合：

- 风险分数（0–100）  
- 逾期率（Overdue Rate）  
- 风险等级（Low / Moderate / High）  

---

### 3. Lender 行为分析

使用聚类方法分析 Lender 风格：

- Conservative（保守）  
- Balanced（中性）  
- Aggressive（激进）  

分析维度包括：

- LVR  
- 利率  
- 贷款期限  
- 二押比例（Second）  

---

### 4. 律师（Partner）风险分析

根据逾期情况计算合作方评分：

- Score（0–100）  
- 等级（A / B / C / D）  

---

### 5. 数据导入与清洗（ETL）

支持 Excel 数据导入，自动完成：

- 字段映射  
- 金额 / 日期清洗  
- 空值处理  
- 数据去重  

---

## 🧠 机器学习模型

- 模型：Logistic Regression  
- 任务：预测贷款是否违约  

输入特征：

- Priority Level  
- Rate  
- LVR  
- Loan Term  
- Principal（对数处理）  

输出结果为违约概率，用于后续风险评分。

---

## 📁 项目结构

```text
.
├── app.py                  # Flask 后端
├── db_config.py            # 共享数据库配置
├── feature_utils.py        # 共享特征清洗逻辑
├── model_predict.py        # 模型预测
├── model_train.py          # 模型训练
├── import.py               # 数据导入脚本
├── static/                 # 前端 JS / CSS
├── templates/              # HTML 页面
├── requirements.txt
└── broker_risk_model.pkl
```

## ⚙️ 运行配置

数据库连接优先读取以下环境变量；未设置时默认回退到本地开发库：

- `PGHOST`，默认 `localhost`
- `PGPORT`，默认 `5433`
- `PGDATABASE`，默认 `lending_db`
- `PGUSER`，默认 `postgres`
- `PGPASSWORD`，默认 `1`

示例：

```powershell
$env:PGHOST="localhost"
$env:PGPORT="5433"
$env:PGDATABASE="lending_db"
$env:PGUSER="postgres"
$env:PGPASSWORD="your_password"
python app.py
```
