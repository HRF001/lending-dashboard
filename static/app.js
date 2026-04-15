let marketChartInstance = null;
let lenderMarketChartInstance = null;
let trendChartInstance = null;
let securityChartInstance = null;

let brokerListData = [];
let brokerRiskData = [];
let brokerHistoryData = [];
let lenderListData = [];
let lenderRiskData = [];
let lenderHistoryData = [];
let lawyerListData = [];
let lawyerRiskData = [];
let lawyerHistoryData = [];

const sortState = {
    "broker-history": { column: "settlement_date", direction: "desc", type: "date" },
    "broker-ranking": { column: "score", direction: "desc", type: "number" },
    "lender-history": { column: "settlement_date", direction: "desc", type: "date" },
    "lender-ranking": { column: "score", direction: "desc", type: "number" },
    "lawyer-history": { column: "settlement_date", direction: "desc", type: "date" },
    "lawyer-ranking": { column: "score", direction: "desc", type: "number" }
};

const chartPalette = {
    market: ["#0c5c4c", "#1f7a66", "#69bea0", "#9dd7c3", "#d8efe7", "#eef8f4"],
    security: ["#0c5c4c", "#1f7a66", "#2f9b7a", "#69bea0", "#9dd7c3", "#d8efe7"],
    trendBorder: "#0f4f43"
};

const gradeSortOrder = {
    "high risk": 4,
    "elevated risk": 3,
    "moderate risk": 2,
    "low risk": 1,
    "aggressive": 3,
    "balanced": 2,
    "conservative": 1,
    "a": 4,
    "b": 3,
    "c": 2,
    "d": 1
};

function formatNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    return new Intl.NumberFormat().format(Math.round(Number(num)));
}

function formatCompactNumber(num) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    const value = Number(num);
    if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(0)}k`;
    return formatNumber(value);
}

function formatMillions(num, digits = 1) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    const value = Number(num);
    return (value / 1000000).toFixed(digits).replace(/\.0+$/, "");
}

function formatDecimal(num, digits = 2) {
    if (num === null || num === undefined || isNaN(num)) return "-";
    return Number(num).toFixed(digits);
}

function formatDate(value) {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "-";
    return date.toLocaleDateString();
}

function renderOverview(data) {
    document.getElementById("totalDeals").textContent = formatNumber(data.total_deals);
    document.getElementById("totalPrincipal").textContent = formatMillions(data.total_principal, 1);
    document.getElementById("avgPrincipal").textContent = formatMillions(data.avg_principal, 1);
}

function titleCaseLabel(value) {
    return String(value ?? "")
        .toLowerCase()
        .replace(/\b\w/g, char => char.toUpperCase());
}

function syncLegendState(containerId, chart) {
    const legend = document.getElementById(containerId);
    if (!legend || !chart) return;

    legend.querySelectorAll(".chart-legend-item").forEach(item => {
        const index = Number(item.dataset.index);
        item.classList.toggle("is-inactive", !chart.getDataVisibility(index));
    });
}

function renderCustomLegend(containerId, labels, colors, chart, mode = "list") {
    const legend = document.getElementById(containerId);
    if (!legend) return;

    const baseClass = mode === "grid" ? "chart-legend-grid" : "chart-legend-list";
    const extraClasses = Array.from(legend.classList).filter(name =>
        name !== "chart-legend-grid" && name !== "chart-legend-list"
    );
    legend.className = [baseClass, ...extraClasses].join(" ");
    legend.innerHTML = labels.map((label, index) => `
        <button type="button" class="chart-legend-item" data-index="${index}">
            <span class="chart-legend-swatch" style="background:${colors[index % colors.length]}"></span>
            <span class="chart-legend-label">${label}</span>
        </button>
    `).join("");

    legend.querySelectorAll(".chart-legend-item").forEach(item => {
        item.addEventListener("click", () => {
            const index = Number(item.dataset.index);
            chart.toggleDataVisibility(index);
            chart.update();
            syncLegendState(containerId, chart);
        });
    });

    syncLegendState(containerId, chart);
}

function filterEntityList(inputId, listId) {
    const input = document.getElementById(inputId);
    const list = document.getElementById(listId);
    if (!input || !list) return;

    const query = input.value.trim().toLowerCase();
    list.querySelectorAll(".entity-item").forEach(item => {
        const text = item.textContent.toLowerCase();
        item.style.display = text.includes(query) ? "" : "none";
    });
}

function sortRows(data, config) {
    const rows = [...data];
    const multiplier = config.direction === "asc" ? 1 : -1;

    rows.sort((a, b) => {
        const left = a?.[config.column];
        const right = b?.[config.column];

        if (config.type === "number") {
            return (Number(left ?? 0) - Number(right ?? 0)) * multiplier;
        }

        if (config.type === "date") {
            const leftTime = left ? new Date(left).getTime() : 0;
            const rightTime = right ? new Date(right).getTime() : 0;
            return ((Number.isNaN(leftTime) ? 0 : leftTime) - (Number.isNaN(rightTime) ? 0 : rightTime)) * multiplier;
        }

        if (config.column === "grade") {
            const leftRank = gradeSortOrder[String(left ?? "").trim().toLowerCase()] ?? -1;
            const rightRank = gradeSortOrder[String(right ?? "").trim().toLowerCase()] ?? -1;
            if (leftRank !== rightRank) {
                return (leftRank - rightRank) * multiplier;
            }
        }

        return String(left ?? "").localeCompare(String(right ?? "")) * multiplier;
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
    if (!current) return;

    if (current.column === column) {
        current.direction = current.direction === "asc" ? "desc" : "asc";
    } else {
        current.column = column;
        current.direction = column === "grade" ? "desc" : (type === "text" ? "asc" : "desc");
        current.type = type;
    }

    if (tableName === "broker-ranking") {
        renderBrokerRankingTable(brokerRiskData);
    } else if (tableName === "broker-history") {
        renderHistoryRows("brokerHistoryTable", brokerHistoryData, "broker", "broker-history");
    } else if (tableName === "lender-ranking") {
        renderLenderRankingTable(lenderRiskData);
    } else if (tableName === "lender-history") {
        renderHistoryRows("lenderHistoryTable", lenderHistoryData, "lender", "lender-history");
    } else if (tableName === "lawyer-ranking") {
        renderLawyerRankingTable(lawyerRiskData);
    } else if (tableName === "lawyer-history") {
        renderHistoryRows("lawyerHistoryTable", lawyerHistoryData, "lawyer", "lawyer-history");
    }
}

function renderEntityList(containerId, items, key, onSelect) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.innerHTML = "";

    if (!Array.isArray(items) || items.length === 0) {
        container.innerHTML = `<div class="entity-item entity-empty">No data available.</div>`;
        return;
    }

    items.forEach((item, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "entity-item";
        button.dataset.entity = item[key];
        button.innerHTML = `
            <span class="entity-name">${item[key]}</span>
            <span class="entity-meta">Deals: ${item.deals} · Principal: ${formatCompactNumber(item.principal ?? item.total)}</span>
        `;
        button.addEventListener("click", () => onSelect(item[key]));
        container.appendChild(button);

        if (index === 0) {
            setTimeout(() => button.click(), 0);
        }
    });
}

function setActiveEntity(containerId, name) {
    const container = document.getElementById(containerId);
    if (!container) return;

    container.querySelectorAll(".entity-item").forEach(item => {
        item.classList.toggle("active", item.dataset.entity === name);
    });
}

function renderHistoryRows(tbodyId, rows, mode, tableName) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9">No history records found.</td></tr>`;
        if (tableName) updateSortHeaders(tableName);
        return;
    }

    const sortedRows = tableName ? sortRows(rows, sortState[tableName]) : rows;

    sortedRows.forEach(row => {
        const tr = document.createElement("tr");

        if (mode === "lawyer") {
            tr.innerHTML = `
                <td>${row.matter_no ?? "-"}</td>
                <td>${row.broker ?? "-"}</td>
                <td>${row.lender ?? "-"}</td>
                <td>${formatCompactNumber(row.principal)}</td>
                <td>${formatDecimal(row.rate, 2)}</td>
                <td>${formatDecimal(row.lvr, 2)}</td>
                <td>${formatDate(row.settlement_date)}</td>
                <td>${formatDate(row.repayment_date)}</td>
                <td>${row.status ?? "-"}</td>
            `;
        } else {
            tr.innerHTML = `
                <td>${row.matter_no ?? "-"}</td>
                <td>${row.counterparty ?? "-"}</td>
                <td>${formatCompactNumber(row.principal)}</td>
                <td>${formatDecimal(row.rate, 2)}</td>
                <td>${formatDecimal(row.lvr, 2)}</td>
                <td>${row.security_type ?? "-"}</td>
                <td>${formatDate(row.settlement_date)}</td>
                <td>${formatDate(row.repayment_date)}</td>
                <td>${row.status ?? "-"}</td>
            `;
        }

        tbody.appendChild(tr);
    });

    if (tableName) updateSortHeaders(tableName);
}

function renderBrokerSnapshot(name) {
    const tbody = document.getElementById("brokerSnapshotTable");
    if (!tbody) return;
    tbody.innerHTML = "";

    const row = brokerRiskData.find(item => item.broker === name);
    if (!row) {
        tbody.innerHTML = `<tr><td colspan="8">No broker risk snapshot available.</td></tr>`;
        return;
    }

    let scoreClass = "score-low";
    if (row.score >= 75) scoreClass = "score-high";
    else if (row.score >= 50) scoreClass = "score-mid";

    tbody.innerHTML = `
        <tr>
            <td>${row.broker}</td>
            <td>${row.deals}</td>
            <td>${formatCompactNumber(row.principal)}</td>
            <td>${formatDecimal(row.lvr, 2)}</td>
            <td>${formatDecimal(row.rate, 2)}</td>
            <td>${formatDecimal(row.overdue_rate, 1)}%</td>
            <td class="${scoreClass}">${formatDecimal(row.score, 1)}</td>
            <td><strong>${row.grade}</strong></td>
        </tr>
    `;
}

function renderBrokerRankingTable(rows) {
    const tbody = document.getElementById("brokerRankingTable");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8">No broker ranking data available.</td></tr>`;
        return;
    }

    sortRows(rows, sortState["broker-ranking"]).forEach(row => {
        let scoreClass = "score-low";
        if (row.score >= 75) scoreClass = "score-high";
        else if (row.score >= 50) scoreClass = "score-mid";

        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${row.broker}</td>
            <td>${row.deals}</td>
            <td>${formatCompactNumber(row.principal)}</td>
            <td>${formatDecimal(row.lvr, 2)}</td>
            <td>${formatDecimal(row.rate, 2)}</td>
            <td>${formatDecimal(row.overdue_rate, 1)}%</td>
            <td class="${scoreClass}">${formatDecimal(row.score, 1)}</td>
            <td><strong>${row.grade}</strong></td>
        `;
        tbody.appendChild(tr);
    });

    updateSortHeaders("broker-ranking");
}

function renderLenderSnapshot(name) {
    const tbody = document.getElementById("lenderSnapshotTable");
    if (!tbody) return;
    tbody.innerHTML = "";

    const row = lenderRiskData.find(item => item.lender === name);
    if (!row) {
        tbody.innerHTML = `<tr><td colspan="9">No lender risk snapshot available.</td></tr>`;
        return;
    }

    tbody.innerHTML = `
        <tr>
            <td>${row.lender}</td>
            <td>${row.deals}</td>
            <td>${formatCompactNumber(row.principal)}</td>
            <td>${formatDecimal(row.lvr, 2)}</td>
            <td>${formatDecimal(row.rate, 2)}</td>
            <td>${formatDecimal(row.term, 0)}</td>
            <td>${formatDecimal(row.second_share, 1)}%</td>
            <td>${formatDecimal(row.score, 1)}</td>
            <td><strong>${row.grade}</strong></td>
        </tr>
    `;
}

function renderLenderRankingTable(rows) {
    const tbody = document.getElementById("lenderRankingTable");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="9">No lender ranking data available.</td></tr>`;
        return;
    }

    sortRows(rows, sortState["lender-ranking"]).forEach(row => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${row.lender}</td>
            <td>${row.deals}</td>
            <td>${formatCompactNumber(row.principal)}</td>
            <td>${formatDecimal(row.lvr, 2)}</td>
            <td>${formatDecimal(row.rate, 2)}</td>
            <td>${formatDecimal(row.term, 0)}</td>
            <td>${formatDecimal(row.second_share, 1)}%</td>
            <td>${formatDecimal(row.score, 1)}</td>
            <td><strong>${row.grade}</strong></td>
        `;
        tbody.appendChild(tr);
    });

    updateSortHeaders("lender-ranking");
}

function renderLawyerSnapshot(name) {
    const tbody = document.getElementById("lawyerSnapshotTable");
    const statusEl = document.getElementById("partnerRiskStatus");
    if (!tbody || !statusEl) return;
    tbody.innerHTML = "";

    const row = lawyerRiskData.find(item => item.partner_name === name);
    if (!row) {
        statusEl.textContent = "No lawyer score snapshot available for this lawyer.";
        tbody.innerHTML = `<tr><td colspan="5">No lawyer score snapshot available.</td></tr>`;
        return;
    }

    statusEl.textContent = `Showing score snapshot for ${name}`;
    tbody.innerHTML = `
        <tr>
            <td>${row.partner_name}</td>
            <td>${row.deals}</td>
            <td>${formatDecimal(Number(row.overdue_rate) * 100, 2)}%</td>
            <td>${row.score}</td>
            <td><strong>${row.grade}</strong></td>
        </tr>
    `;
}

function renderLawyerRankingTable(rows) {
    const tbody = document.getElementById("lawyerRankingTable");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!Array.isArray(rows) || rows.length === 0) {
        tbody.innerHTML = `<tr><td colspan="5">No lawyer ranking data available.</td></tr>`;
        return;
    }

    sortRows(rows, sortState["lawyer-ranking"]).forEach(row => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${row.partner_name}</td>
            <td>${row.deals}</td>
            <td>${formatDecimal(Number(row.overdue_rate) * 100, 2)}%</td>
            <td>${row.score}</td>
            <td><strong>${row.grade}</strong></td>
        `;
        tbody.appendChild(tr);
    });

    updateSortHeaders("lawyer-ranking");
}

async function selectBroker(name) {
    setActiveEntity("brokerList", name);
    document.getElementById("brokerDetailTitle").textContent = `${name} History`;
    document.getElementById("brokerDetailSubtitle").textContent = "Recent matters and lending records for the selected broker.";
    renderBrokerSnapshot(name);

    const res = await fetch(`/api/broker-history?broker=${encodeURIComponent(name)}`);
    const data = await res.json();
    brokerHistoryData = data;
    renderHistoryRows("brokerHistoryTable", brokerHistoryData, "broker", "broker-history");
}

async function selectLender(name) {
    setActiveEntity("lenderList", name);
    document.getElementById("lenderDetailTitle").textContent = `${name} History`;
    document.getElementById("lenderDetailSubtitle").textContent = "Recent matters and lending records for the selected lender.";
    renderLenderSnapshot(name);

    const res = await fetch(`/api/lender-history?lender=${encodeURIComponent(name)}`);
    const data = await res.json();
    lenderHistoryData = data;
    renderHistoryRows("lenderHistoryTable", lenderHistoryData, "lender", "lender-history");
}

async function selectLawyer(name) {
    setActiveEntity("lawyerList", name);
    document.getElementById("lawyerDetailTitle").textContent = `${name} History`;
    document.getElementById("lawyerDetailSubtitle").textContent = "Recent matters handled by the selected lawyer.";
    renderLawyerSnapshot(name);

    const res = await fetch(`/api/lawyer-history?lawyer=${encodeURIComponent(name)}`);
    const data = await res.json();
    lawyerHistoryData = data;
    renderHistoryRows("lawyerHistoryTable", lawyerHistoryData, "lawyer", "lawyer-history");
}

function renderMarketChart(data) {
    const labels = data.map(x => titleCaseLabel(x.type));
    const values = data.map(x => x.principal);
    const ctx = document.getElementById("marketChart").getContext("2d");

    if (marketChartInstance) marketChartInstance.destroy();

    marketChartInstance = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: chartPalette.market,
                borderColor: "rgba(255, 253, 248, 0.95)",
                borderWidth: 3,
                hoverOffset: 10,
                radius: "78%"
            }]
        },
        options: {
            maintainAspectRatio: false,
            responsive: true,
            cutout: "50%",
            layout: {
                padding: {
                    left: 8,
                    right: 8
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    titleFont: { size: 16, weight: "700" },
                    bodyFont: { size: 15 },
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label(context) {
                            return `${context.label}: ${formatMillions(context.parsed || 0, 1)} million AUD`;
                        }
                    }
                }
            }
        }
    });

    renderCustomLegend("marketLegend", labels, chartPalette.market, marketChartInstance, "list");
}

function renderTrendChart(data) {
    const labels = data.map(x => x.date);
    const values = data.map(x => x.principal);
    const ctx = document.getElementById("trendChart").getContext("2d");
    const gradient = ctx.createLinearGradient(0, 0, 0, 320);
    gradient.addColorStop(0, "rgba(12, 92, 76, 0.38)");
    gradient.addColorStop(1, "rgba(12, 92, 76, 0.05)");

    if (trendChartInstance) trendChartInstance.destroy();

    trendChartInstance = new Chart(ctx, {
        type: "bar",
        data: {
            labels,
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
                        font: { size: 15 },
                        usePointStyle: true,
                        pointStyle: "rectRounded"
                    }
                },
                tooltip: {
                    titleFont: { size: 16, weight: "700" },
                    bodyFont: { size: 15 },
                    padding: 12,
                    displayColors: false,
                    callbacks: {
                        label(context) {
                            return `${context.dataset.label}: ${formatMillions(context.parsed.y, 1)} million AUD`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: {
                        color: "#5c675f",
                        font: { size: 16 },
                        maxRotation: 45,
                        minRotation: 45
                    }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: "rgba(28, 36, 33, 0.08)" },
                    title: {
                        display: true,
                        text: "Million AUD",
                        color: "#5c675f",
                        font: { size: 16, weight: "600" }
                    },
                    ticks: {
                        color: "#5c675f",
                        font: { size: 16 },
                        maxTicksLimit: 5,
                        callback(value) { return formatMillions(value, 1); }
                    }
                }
            }
        }
    });
}

function renderLenderMarketChart(data) {
    const labels = data.map(x => x.type);
    const values = data.map(x => x.principal);
    const ctx = document.getElementById("lenderMarketChart").getContext("2d");

    if (lenderMarketChartInstance) lenderMarketChartInstance.destroy();

    lenderMarketChartInstance = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: chartPalette.market,
                borderColor: "rgba(255, 253, 248, 0.95)",
                borderWidth: 3,
                hoverOffset: 10,
                radius: "78%"
            }]
        },
        options: {
            maintainAspectRatio: false,
            responsive: true,
            cutout: "50%",
            layout: {
                padding: {
                    left: 8,
                    right: 8
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    titleFont: { size: 16, weight: "700" },
                    bodyFont: { size: 15 },
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label(context) {
                            return `${context.label}: ${formatMillions(context.parsed || 0, 1)} million AUD`;
                        }
                    }
                }
            }
        }
    });

    renderCustomLegend("lenderLegend", labels, chartPalette.market, lenderMarketChartInstance, "list");
}

function renderSecurityChart(data) {
    if (!Array.isArray(data)) return;
    const labels = data.map(d => d.type);
    const values = data.map(d => d.count);
    const ctx = document.getElementById("securityChart").getContext("2d");

    if (securityChartInstance) securityChartInstance.destroy();

    securityChartInstance = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: chartPalette.security,
                borderColor: "rgba(255, 253, 248, 0.95)",
                borderWidth: 3,
                hoverOffset: 10,
                radius: "78%"
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: "50%",
            layout: {
                padding: {
                    right: 8,
                    left: 8
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    titleFont: { size: 16, weight: "700" },
                    bodyFont: { size: 15 },
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        label(context) {
                            return `${context.label}: ${formatNumber(context.parsed)}`;
                        }
                    }
                }
            }
        }
    });

    renderCustomLegend("securityLegend", labels, chartPalette.security, securityChartInstance, "grid");
}

async function loadOverviewPage() {
    try {
        const [overviewRes, marketRes, lenderMarketRes, trendRes, securityRes] = await Promise.all([
            fetch("/api/overview"),
            fetch("/api/market-structure"),
            fetch("/api/lender-market-structure"),
            fetch("/api/trend"),
            fetch("/api/security-type")
        ]);

        const overview = await overviewRes.json();
        const market = await marketRes.json();
        const lenderMarket = await lenderMarketRes.json();
        const trend = await trendRes.json();
        const security = await securityRes.json();

        renderOverview(overview);
        renderMarketChart(market);
        renderLenderMarketChart(lenderMarket);
        renderTrendChart(trend);
        renderSecurityChart(security);
    } catch (error) {
        console.error("Overview load failed:", error);
    }
}

async function loadBrokerPage() {
    try {
        const [brokersRes, brokerRiskRes] = await Promise.all([
            fetch("/api/top-brokers"),
            fetch("/api/broker-risk")
        ]);

        brokerListData = await brokersRes.json();
        brokerRiskData = await brokerRiskRes.json();
        renderEntityList("brokerList", brokerListData, "broker", selectBroker);
        renderBrokerRankingTable(brokerRiskData);
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

        lenderListData = await lendersRes.json();
        lenderRiskData = await lenderRiskRes.json();
        renderEntityList("lenderList", lenderListData, "lender", selectLender);
        renderLenderRankingTable(lenderRiskData);
    } catch (error) {
        console.error("Lender page load failed:", error);
    }
}

async function loadLawyerPage() {
    try {
        const [lawyersRes, lawyerRiskRes] = await Promise.all([
            fetch("/api/all-lawyers"),
            fetch("/api/partner-risk")
        ]);

        lawyerListData = await lawyersRes.json();
        lawyerRiskData = await lawyerRiskRes.json();
        renderEntityList("lawyerList", lawyerListData, "lawyer", selectLawyer);
        renderLawyerRankingTable(lawyerRiskData);
    } catch (error) {
        console.error("Lawyer page load failed:", error);
    }
}

async function refreshData() {
    const page = document.body.dataset.page;

    if (page === "overview") {
        await loadOverviewPage();
    } else if (page === "brokers") {
        await loadBrokerPage();
    } else if (page === "lenders") {
        await loadLenderPage();
    } else if (page === "lawyers") {
        await loadLawyerPage();
    }
}

document.addEventListener("DOMContentLoaded", () => {
    document.addEventListener("click", handleSortClick);
    refreshData();
});
