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
        if (b.score >= 45) scoreClass = "score-high";
        else if (b.score >= 30) scoreClass = "score-mid";

        table.innerHTML += `
            <tr>
                <td>${b.broker}</td>
                <td>${b.deals}</td>
                <td>${formatNumber(b.principal)}</td>
                <td>${b.lvr ? b.lvr.toFixed(2) : "0.00"}</td>
                <td>${b.rate ? b.rate.toFixed(2) : "0.00"}</td>
                <td class="${scoreClass}">${b.score.toFixed(1)}</td>
                <td><strong>${b.grade}</strong></td>
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
    try {
        const [
            summaryRes,
            marketRes,
            trendRes,
            brokersRes,
            brokerRiskRes,
            lendersRes,
            lenderRiskRes
        ] = await Promise.all([
            fetch("/api/summary"),
            fetch("/api/market-structure"),
            fetch("/api/settlement-trend"),
            fetch("/api/top-brokers"),
            fetch("/api/broker-risk"),
            fetch("/api/top-lenders"),     
            fetch("/api/lender-risk")      
        ]);

        const summary = await summaryRes.json();
        const market = await marketRes.json();
        const trend = await trendRes.json();
        const brokers = await brokersRes.json();
        const brokerRisk = await brokerRiskRes.json();
        const lenders = await lendersRes.json();       
        const lenderRisk = await lenderRiskRes.json();  

        renderTopBrokers(brokers);
        renderRiskTable(brokerRisk);

        renderTopLenders(lenders);
        renderLenderRiskTable(lenderRisk);

        renderMarketChart(market);
        renderTrendChart(trend);

    } catch (error) {
        console.error("刷新数据失败:", error);
    }
}

async function loadAll() {
    await loadOverview();
    await loadTopBrokers();
    await loadRisk();
    await loadMarketStructure();
    await loadTrend();
}

function renderTopLenders(lenders) {
    const lenderList = document.getElementById("lenderList");
    lenderList.innerHTML = "";

    lenders.forEach(lender => {
        const li = document.createElement("li");
        li.textContent = `${lender.lender} | Deals: ${lender.deals} | Principal: ${Number(lender.principal).toLocaleString()}`;
        lenderList.appendChild(li);
    });
}

function renderLenderRiskTable(lenders) {
    const tableBody = document.getElementById("lenderRiskTable");
    tableBody.innerHTML = "";

    lenders.forEach(lender => {
        const row = document.createElement("tr");

        row.innerHTML = `
            <td>${lender.lender}</td>
            <td>${lender.deals}</td>
            <td>${Number(lender.principal).toLocaleString()}</td>
            <td>${Number(lender.lvr).toFixed(2)}</td>
            <td>${Number(lender.rate).toFixed(2)}</td>
            <td>${Number(lender.score).toFixed(1)}</td>
            <td>${lender.grade}</td>
        `;

        tableBody.appendChild(row);
    });
}

refreshData();
loadAll();