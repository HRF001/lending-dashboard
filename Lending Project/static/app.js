let marketChartInstance = null;
let trendChartInstance = null;

function formatNumber(num) {
    return new Intl.NumberFormat().format(Math.round(num));
}

async function loadOverview() {
    const res = await fetch("/api/overview");
    const data = await res.json();

    document.getElementById("totalDeals").textContent = formatNumber(data.total_deals);
    document.getElementById("totalPrincipal").textContent = formatNumber(data.total_principal);
    document.getElementById("avgPrincipal").textContent = formatNumber(data.avg_principal);
}

async function loadTopBrokers() {
    const res = await fetch("/api/top-brokers");
    const data = await res.json();

    const list = document.getElementById("brokerList");
    list.innerHTML = "";

    data.forEach(item => {
        const li = document.createElement("li");
        li.textContent = `${item.broker} | Deals: ${item.deals} | Principal: ${formatNumber(item.total)}`;
        list.appendChild(li);
    });
}

async function loadRisk() {
    const res = await fetch("/api/broker-risk");
    const data = await res.json();

    const table = document.getElementById("riskTable");
    table.innerHTML = "";

    if (!Array.isArray(data)) {
        table.innerHTML = `<tr><td colspan="6">加载失败：${data.error || "未知错误"}</td></tr>`;
        return;
    }

    data.forEach(b => {
        let scoreClass = "score-low";
        if (b.score >= 0.45) scoreClass = "score-high";
        else if (b.score >= 0.20) scoreClass = "score-mid";

        table.innerHTML += `
            <tr>
                <td>${b.broker}</td>
                <td>${b.deals}</td>
                <td>${formatNumber(b.principal)}</td>
                <td>${b.lvr ? b.lvr.toFixed(2) : "0.00"}</td>
                <td>${b.rate ? b.rate.toFixed(2) : "0.00"}</td>
                <td class="${scoreClass}">${b.score.toFixed(3)}</td>
            </tr>
        `;
    });
}

async function loadMarketStructure() {
    const res = await fetch("/api/market-structure");
    const data = await res.json();

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

async function loadTrend() {
    const res = await fetch("/api/trend");
    const data = await res.json();

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
    await fetch("/api/refresh");
    await loadAll();
}

async function loadAll() {
    await loadOverview();
    await loadTopBrokers();
    await loadRisk();
    await loadMarketStructure();
    await loadTrend();
}

loadAll();