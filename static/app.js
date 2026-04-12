let marketChartInstance = null;
let trendChartInstance = null;

function formatNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    return new Intl.NumberFormat().format(Math.round(Number(num)));
}

function formatDecimal(num, digits = 2) {
    if (num === null || num === undefined || isNaN(num)) return "0.00";
    return Number(num).toFixed(digits);
}

function renderOverview(data) {
    document.getElementById("totalDeals").textContent = formatNumber(data.total_deals);
    document.getElementById("totalPrincipal").textContent = formatNumber(data.total_principal);
    document.getElementById("avgPrincipal").textContent = formatNumber(data.avg_principal);
}

function renderTopBrokers(data) {
    const list = document.getElementById("brokerList");
    list.innerHTML = "";

    data.forEach(item => {
        const li = document.createElement("li");
        li.textContent = `${item.broker} | Deals: ${item.deals} | Principal: ${formatNumber(item.principal ?? item.total)}`;
        list.appendChild(li);
    });
}

function renderRiskTable(data) {
    const table = document.getElementById("riskTable");
    table.innerHTML = "";

    if (!Array.isArray(data)) {
        table.innerHTML = `<tr><td colspan="8">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    data.forEach(b => {
        let scoreClass = "score-low";
        if (b.score >= 75) scoreClass = "score-high";
        else if (b.score >= 50) scoreClass = "score-mid";

        table.innerHTML += `
            <tr>
                <td>${b.broker ?? "-"}</td>
                <td>${b.deals ?? "-"}</td>
                <td>${formatNumber(b.principal)}</td>
                <td>${formatDecimal(b.lvr, 2)}</td>
                <td>${formatDecimal(b.rate, 2)}</td>
                <td>${formatDecimal(b.overdue_rate, 1)}%</td>
                <td class="${scoreClass}">${formatDecimal(b.score, 1)}</td>
                <td><strong>${b.grade ?? "-"}</strong></td>
            </tr>
        `;
    });
}

function renderTopLenders(data) {
    const lenderList = document.getElementById("lenderList");
    lenderList.innerHTML = "";

    if (!Array.isArray(data)) return;

    data.forEach(item => {
        const li = document.createElement("li");
        li.textContent = `${item.lender} | Deals: ${item.deals} | Principal: ${formatNumber(item.principal)}`;
        lenderList.appendChild(li);
    });
}

function renderLenderAggressivenessTable(data) {
    const tableBody = document.getElementById("lenderRiskTable");
    tableBody.innerHTML = "";

    if (!Array.isArray(data)) {
        tableBody.innerHTML = `<tr><td colspan="9">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    data.forEach(item => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${item.lender ?? "-"}</td>
            <td>${item.deals ?? "-"}</td>
            <td>${Number(item.principal).toLocaleString()}</td>
            <td>${Number(item.lvr).toFixed(2)}</td>
            <td>${Number(item.rate).toFixed(2)}</td>
            <td>${Number(item.term).toFixed(0)}</td>
            <td>${Number(item.second_share).toFixed(1)}%</td>
            <td>${Number(item.score).toFixed(1)}</td>
            <td>${item.grade ?? "-"}</td>
        `;

        tableBody.appendChild(row);
    });
}

function renderMarketChart(data) {
    const labels = data.map(x => x.type);
    const values = data.map(x => x.principal);

    const ctx = document.getElementById("marketChart").getContext("2d");

    if (marketChartInstance) {
        marketChartInstance.destroy();
    }

    marketChartInstance = new Chart(ctx, {
        type: "pie",
        data: {
            labels: labels,
            datasets: [{
                data: values
            }]
        },
        options: {
            maintainAspectRatio: false, 
            responsive: true,
            plugins: {
                legend: {
                    position: "right",
                    labels: {
                        font: {
                            size: 18   // 👈 调大（可以试 14 / 16 / 18）
                        },
                        boxWidth: 30,   // 👈 颜色块变大
                        padding: 15     // 👈 每一行间距
                    }   // 👈 不要放 top，会挤
            }
        }
        }
    });
}

function renderTrendChart(data) {
    const labels = data.map(x => x.date);
    const values = data.map(x => x.principal);

    const ctx = document.getElementById("trendChart").getContext("2d");

    if (trendChartInstance) {
        trendChartInstance.destroy();
    }

    trendChartInstance = new Chart(ctx, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [{
                label: "Monthly Principal",
                data: values
            }]
        },
        options: {
            responsive: true,
            scales: {
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

async function refreshData() {
    try {
        const [
            overviewRes,
            marketRes,
            trendRes,
            brokersRes,
            brokerRiskRes,
            lendersRes,
            lenderRiskRes
        ] = await Promise.all([
            fetch("/api/overview"),
            fetch("/api/market-structure"),
            fetch("/api/trend"),
            fetch("/api/top-brokers"),
            fetch("/api/broker-risk"),
            fetch("/api/top-lenders"),
            fetch("/api/lender-aggressiveness")
        ]);

        const overview = await overviewRes.json();
        const market = await marketRes.json();
        const trend = await trendRes.json();
        const brokers = await brokersRes.json();
        const brokerRisk = await brokerRiskRes.json();
        const lenders = await lendersRes.json();
        const lenderRisk = await lenderRiskRes.json();

        renderOverview(overview);
        renderTopBrokers(brokers);
        renderRiskTable(brokerRisk);
        renderTopLenders(lenders);
        renderLenderAggressivenessTable(lenderRisk);
        renderMarketChart(market);
        renderTrendChart(trend);

        console.log("lenders:", lenders);
        console.log("lenderRisk:", lenderRisk);
    } catch (error) {
        console.error("刷新数据失败:", error);
    }
}

async function loadSecurityChart() {
    const res = await fetch("/api/security-type");
    const data = await res.json();

    const labels = data.map(d => d.type);
    const values = data.map(d => d.count);

    new Chart(document.getElementById("securityChart"), {
        type: "pie",
        data: {
            labels: labels,
            datasets: [{
                data: values
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false, 
            plugins: {
                legend: {
                    position: "right",
                    labels: {
                        font: {
                            size: 18   // 👈 调大（可以试 14 / 16 / 18）
                        },
                        boxWidth: 30,   // 👈 颜色块变大
                        padding: 15     // 👈 每一行间距
                    }
                }
            }
        }
    });
}

async function loadPartnerRisk() {
    const statusEl = document.getElementById("partnerRiskStatus");
    const tbody = document.querySelector("#partnerRiskTable tbody");

    statusEl.textContent = "Loading...";
    tbody.innerHTML = "";

    try {
        const response = await fetch("/api/partner-risk");
        const data = await response.json();

        console.log("partner risk response:", data);

        if (!response.ok) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        if (!Array.isArray(data) || data.length === 0) {
            statusEl.textContent = "No partner risk data found.";
            return;
        }

        data.forEach(row => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${row.partner_name ?? ""}</td>
                <td>${row.deals ?? ""}</td>
                <td>${(Number(row.overdue_rate) * 100).toFixed(2)}%</td>
                <td>${row.score ?? ""}</td>
                <td>${row.grade ?? ""}</td>
            `;
            tbody.appendChild(tr);
        });

        statusEl.textContent = `Loaded ${data.length} partners`;
    } catch (error) {
        console.error("Partner risk load error:", error);
        statusEl.textContent = "Failed to load partner risk data: " + error.message;
    }
}

function formatPercent(value) {
    if (value === null || value === undefined || value === "") {
        return "";
    }
    return (Number(value) * 100).toFixed(2) + "%";
}

function renderGradeBadge(grade) {
    let color = "#999";

    if (grade === "A") color = "#2e7d32";
    else if (grade === "B") color = "#1565c0";
    else if (grade === "C") color = "#ef6c00";
    else if (grade === "D") color = "#c62828";

    return `<span style="
        display:inline-block;
        min-width:32px;
        text-align:center;
        padding:4px 8px;
        border-radius:12px;
        color:white;
        background:${color};
        font-weight:bold;
    ">${grade}</span>`;
}

document.addEventListener("DOMContentLoaded", () => {
    refreshData();
    loadPartnerRisk();   
    loadSecurityChart();
});