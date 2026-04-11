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
        table.innerHTML = `<tr><td colspan="7">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    data.forEach(b => {
        let scoreClass = "score-low";
        if (b.score >= 45) scoreClass = "score-high";
        else if (b.score >= 30) scoreClass = "score-mid";

        table.innerHTML += `
            <tr>
                <td>${b.broker ?? "-"}</td>
                <td>${b.deals ?? "-"}</td>
                <td>${formatNumber(b.principal)}</td>
                <td>${formatDecimal(b.lvr, 2)}</td>
                <td>${formatDecimal(b.rate, 2)}</td>
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

function renderLenderRiskTable(data) {
    const tableBody = document.getElementById("lenderRiskTable");
    tableBody.innerHTML = "";

    if (!Array.isArray(data)) {
        tableBody.innerHTML = `<tr><td colspan="7">加载失败</td></tr>`;
        return;
    }

    data.forEach(item => {
        let scoreClass = "score-low";
        if (item.score >= 45) scoreClass = "score-high";
        else if (item.score >= 30) scoreClass = "score-mid";

        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${item.lender ?? "-"}</td>
            <td>${item.deals ?? "-"}</td>
            <td>${formatNumber(item.principal)}</td>
            <td>${formatDecimal(item.lvr, 2)}</td>
            <td>${formatDecimal(item.rate, 2)}</td>
            <td class="${scoreClass}">${formatDecimal(item.score, 1)}</td>
            <td><strong>${item.grade ?? "-"}</strong></td>
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
            responsive: true
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
        type: "line",
        data: {
            labels: labels,
            datasets: [{
                label: "Principal",
                data: values
            }]
        },
        options: {
            responsive: true
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
            fetch("/api/lender-risk")
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
        renderLenderRiskTable(lenderRisk);
        renderMarketChart(market);
        renderTrendChart(trend);

        console.log("lenders:", lenders);
        console.log("lenderRisk:", lenderRisk);
    } catch (error) {
        console.error("刷新数据失败:", error);
    }
}

document.addEventListener("DOMContentLoaded", refreshData);