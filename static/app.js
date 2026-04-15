let marketChartInstance = null;
let trendChartInstance = null;
let securityChartInstance = null;
let brokerRiskData = [];
let lenderRiskData = [];
let lawyerRiskData = [];

const sortState = {
    broker: { column: "score", direction: "desc", type: "number" },
    lender: { column: "score", direction: "desc", type: "number" },
    lawyer: { column: "score", direction: "desc", type: "number" }
};

const chartPalette = {
    market: ["#0c5c4c", "#3c7c69", "#7ca892", "#b66a2c", "#d4a46c", "#ead6b8"],
    security: ["#b66a2c", "#d38b47", "#e6b46d", "#0c5c4c", "#4c8978", "#9ab9ab"],
    trendLine: "#0c5c4c",
    trendFill: "rgba(12, 92, 76, 0.18)",
    trendBorder: "#0f4f43"
};

function formatNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    return new Intl.NumberFormat().format(Math.round(Number(num)));
}

function formatCompactNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return "-";

    const value = Number(num);
    if (Math.abs(value) >= 1000) {
        return `${(value / 1000).toFixed(0)}k`;
    }
    return formatNumber(value);
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

    if (!Array.isArray(data) || data.length === 0) {
        list.innerHTML = "<li>No broker data available.</li>";
        return;
    }

    data.forEach(item => {
        const li = document.createElement("li");
        li.textContent = `${item.broker} | Deals: ${item.deals} | Principal: ${formatCompactNumber(item.principal ?? item.total)}`;
        list.appendChild(li);
    });
}

function filterTableRows(inputId, tableBodyId, columnIndex = 0) {
    const input = document.getElementById(inputId);
    const tbody = document.getElementById(tableBodyId);

    if (!input || !tbody) return;

    const query = input.value.trim().toLowerCase();
    const rows = tbody.querySelectorAll("tr");

    rows.forEach(row => {
        const cells = row.querySelectorAll("td");
        const text = (cells[columnIndex]?.textContent || "").toLowerCase();
        row.style.display = text.includes(query) ? "" : "none";
    });
}

function sortRows(data, config) {
    const rows = [...data];
    const multiplier = config.direction === "asc" ? 1 : -1;

    rows.sort((a, b) => {
        const aValue = a?.[config.column];
        const bValue = b?.[config.column];

        if (config.type === "number") {
            const left = Number(aValue ?? 0);
            const right = Number(bValue ?? 0);
            return (left - right) * multiplier;
        }

        return String(aValue ?? "").localeCompare(String(bValue ?? "")) * multiplier;
    });

    return rows;
}

function updateSortHeaders(tableName) {
    document.querySelectorAll(`th.sortable[data-table="${tableName}"]`).forEach(th => {
        th.classList.remove("sort-asc", "sort-desc");

        if (th.dataset.column === sortState[tableName].column) {
            th.classList.add(sortState[tableName].direction === "asc" ? "sort-asc" : "sort-desc");
        }
    });
}

function handleSortClick(event) {
    const th = event.target.closest("th.sortable");
    if (!th) return;

    const tableName = th.dataset.table;
    const column = th.dataset.column;
    const type = th.dataset.type || "text";
    const current = sortState[tableName];

    if (current.column === column) {
        current.direction = current.direction === "asc" ? "desc" : "asc";
    } else {
        current.column = column;
        current.direction = type === "text" ? "asc" : "desc";
        current.type = type;
    }

    updateSortHeaders(tableName);

    if (tableName === "broker") {
        renderRiskTable(brokerRiskData);
    } else if (tableName === "lender") {
        renderLenderAggressivenessTable(lenderRiskData);
    } else if (tableName === "lawyer") {
        renderLawyerRiskTable(lawyerRiskData);
    }
}

function renderRiskTable(data) {
    brokerRiskData = Array.isArray(data) ? [...data] : [];
    const table = document.getElementById("riskTable");
    table.innerHTML = "";

    if (!Array.isArray(data)) {
        table.innerHTML = `<tr><td colspan="8">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    const sortedData = sortRows(data, sortState.broker);

    sortedData.forEach(b => {
        let scoreClass = "score-low";
        if (b.score >= 75) scoreClass = "score-high";
        else if (b.score >= 50) scoreClass = "score-mid";

        table.innerHTML += `
            <tr>
                <td>${b.broker ?? "-"}</td>
                <td>${b.deals ?? "-"}</td>
                <td>${formatCompactNumber(b.principal)}</td>
                <td>${formatDecimal(b.lvr, 2)}</td>
                <td>${formatDecimal(b.rate, 2)}</td>
                <td>${formatDecimal(b.overdue_rate, 1)}%</td>
                <td class="${scoreClass}">${formatDecimal(b.score, 1)}</td>
                <td><strong>${b.grade ?? "-"}</strong></td>
            </tr>
        `;
    });

    updateSortHeaders("broker");
    filterTableRows("brokerRiskSearch", "riskTable", 0);
}

function renderTopLenders(data) {
    const lenderList = document.getElementById("lenderList");
    lenderList.innerHTML = "";

    if (!Array.isArray(data) || data.length === 0) {
        lenderList.innerHTML = "<li>No lender data available.</li>";
        return;
    }

    data.forEach(item => {
        const li = document.createElement("li");
        li.textContent = `${item.lender} | Deals: ${item.deals} | Principal: ${formatCompactNumber(item.principal)}`;
        lenderList.appendChild(li);
    });
}

function renderLenderAggressivenessTable(data) {
    lenderRiskData = Array.isArray(data) ? [...data] : [];
    const tableBody = document.getElementById("lenderRiskTable");
    tableBody.innerHTML = "";

    if (!Array.isArray(data)) {
        tableBody.innerHTML = `<tr><td colspan="9">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    const sortedData = sortRows(data, sortState.lender);

    sortedData.forEach(item => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${item.lender ?? "-"}</td>
            <td>${item.deals ?? "-"}</td>
            <td>${formatCompactNumber(item.principal)}</td>
            <td>${Number(item.lvr).toFixed(2)}</td>
            <td>${Number(item.rate).toFixed(2)}</td>
            <td>${Number(item.term).toFixed(0)}</td>
            <td>${Number(item.second_share).toFixed(1)}%</td>
            <td>${Number(item.score).toFixed(1)}</td>
            <td>${item.grade ?? "-"}</td>
        `;

        tableBody.appendChild(row);
    });

    updateSortHeaders("lender");
    filterTableRows("lenderRiskSearch", "lenderRiskTable", 0);
}

function renderLawyerRiskTable(data) {
    lawyerRiskData = Array.isArray(data) ? [...data] : [];
    const tbody = document.getElementById("partnerRiskTableBody");
    tbody.innerHTML = "";

    if (!Array.isArray(data)) {
        return;
    }

    const sortedData = sortRows(data, sortState.lawyer);

    sortedData.forEach(row => {
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

    updateSortHeaders("lawyer");
    filterTableRows("lawyerRiskSearch", "partnerRiskTableBody", 0);
}

function renderMarketChart(data) {
    const labels = data.map(x => x.type);
    const values = data.map(x => x.principal);

    const ctx = document.getElementById("marketChart").getContext("2d");

    if (marketChartInstance) {
        marketChartInstance.destroy();
    }

    marketChartInstance = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: chartPalette.market,
                borderColor: "rgba(255, 253, 248, 0.95)",
                borderWidth: 3,
                hoverOffset: 10
            }]
        },
        options: {
            maintainAspectRatio: false,
            responsive: true,
            cutout: "58%",
            plugins: {
                legend: {
                    position: "right",
                    labels: {
                        font: {
                            size: 15
                        },
                        boxWidth: 18,
                        padding: 16,
                        usePointStyle: true,
                        pointStyle: "circle"
                    }
                },
                tooltip: {
                    callbacks: {
                        label(context) {
                            const value = context.parsed || 0;
                            return `${context.label}: ${formatCompactNumber(value)}`;
                        }
                    }
                }
            }
        }
    });
}

function renderTrendChart(data) {
    const labels = data.map(x => x.date);
    const values = data.map(x => x.principal);

    const ctx = document.getElementById("trendChart").getContext("2d");
    const gradient = ctx.createLinearGradient(0, 0, 0, 320);
    gradient.addColorStop(0, "rgba(12, 92, 76, 0.38)");
    gradient.addColorStop(1, "rgba(12, 92, 76, 0.05)");

    if (trendChartInstance) {
        trendChartInstance.destroy();
    }

    trendChartInstance = new Chart(ctx, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [{
                label: "Monthly Principal",
                data: values,
                backgroundColor: gradient,
                borderColor: chartPalette.trendBorder,
                borderWidth: 2,
                borderRadius: 10,
                borderSkipped: false,
                hoverBackgroundColor: "rgba(12, 92, 76, 0.5)"
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        font: {
                            size: 15
                        },
                        usePointStyle: true,
                        pointStyle: "rectRounded"
                    }
                },
                tooltip: {
                    callbacks: {
                        label(context) {
                            return `${context.dataset.label}: ${formatCompactNumber(context.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        color: "#5c675f",
                        font: {
                            size: 13
                        },
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: {
                        color: "rgba(28, 36, 33, 0.08)"
                    },
                    ticks: {
                        color: "#5c675f",
                        font: {
                            size: 13
                        },
                        callback(value) {
                            return formatCompactNumber(value);
                        }
                    }
                }
            }
        }
    });
}

async function refreshData() {
    const page = document.body.dataset.page;

    if (page === "overview") {
        await loadOverviewPage();
        return;
    }

    if (page === "brokers") {
        await loadBrokerPage();
        return;
    }

    if (page === "lenders") {
        await loadLenderPage();
        return;
    }

    if (page === "lawyers") {
        await loadLawyerPage();
        return;
    }
}

async function loadOverviewPage() {
    try {
        const [overviewRes, marketRes, trendRes, securityRes] = await Promise.all([
            fetch("/api/overview"),
            fetch("/api/market-structure"),
            fetch("/api/trend"),
            fetch("/api/security-type")
        ]);

        const overview = await overviewRes.json();
        const market = await marketRes.json();
        const trend = await trendRes.json();
        const security = await securityRes.json();

        renderOverview(overview);
        renderMarketChart(market);
        renderTrendChart(trend);
        renderSecurityChart(security);
    } catch (error) {
        console.error("刷新数据失败:", error);
    }
}

async function loadBrokerPage() {
    try {
        const [brokersRes, brokerRiskRes] = await Promise.all([
            fetch("/api/top-brokers"),
            fetch("/api/broker-risk")
        ]);

        const brokers = await brokersRes.json();
        const brokerRisk = await brokerRiskRes.json();

        renderTopBrokers(brokers);
        renderRiskTable(brokerRisk);
    } catch (error) {
        console.error("Broker page load failed:", error);
    }
}

async function loadLenderPage() {
    try {
        const [lendersRes, lenderRiskRes] = await Promise.all([
            fetch("/api/top-lenders"),
            fetch("/api/lender-aggressiveness")
        ]);

        const lenders = await lendersRes.json();
        const lenderRisk = await lenderRiskRes.json();

        renderTopLenders(lenders);
        renderLenderAggressivenessTable(lenderRisk);
    } catch (error) {
        console.error("Lender page load failed:", error);
    }
}

async function loadLawyerPage() {
    await loadPartnerRisk();
}

function renderSecurityChart(data) {
    if (!Array.isArray(data)) return;

    const labels = data.map(d => d.type);
    const values = data.map(d => d.count);

    const ctx = document.getElementById("securityChart").getContext("2d");

    if (securityChartInstance) {
        securityChartInstance.destroy();
    }

    securityChartInstance = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: chartPalette.security,
                borderColor: "rgba(255, 253, 248, 0.95)",
                borderWidth: 3,
                hoverOffset: 10
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: "54%",
            plugins: {
                legend: {
                    position: "right",
                    labels: {
                        font: {
                            size: 15
                        },
                        boxWidth: 18,
                        padding: 16,
                        usePointStyle: true,
                        pointStyle: "circle"
                    }
                },
                tooltip: {
                    callbacks: {
                        label(context) {
                            return `${context.label}: ${formatNumber(context.parsed)}`;
                        }
                    }
                }
            }
        }
    });
}

async function loadPartnerRisk() {
    const statusEl = document.getElementById("partnerRiskStatus");
    const tbody = document.getElementById("partnerRiskTableBody");

    statusEl.textContent = "Loading...";
    tbody.innerHTML = "";

    try {
        const response = await fetch("/api/partner-risk");
        const data = await response.json();

        console.log("lawyer risk response:", data);

        if (!response.ok) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        if (!Array.isArray(data) || data.length === 0) {
            statusEl.textContent = "No lawyer risk data found.";
            return;
        }

        renderLawyerRiskTable(data);
        statusEl.textContent = `Loaded ${data.length} lawyers`;
    } catch (error) {
        console.error("Lawyer risk load error:", error);
        statusEl.textContent = "Failed to load lawyer risk data: " + error.message;
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
    document.addEventListener("click", handleSortClick);
    refreshData();
});
