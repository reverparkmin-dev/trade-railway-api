const fs = require("fs");
const path = require("path");

// 1) 전체 수출 데이터 읽기
const allDataPath = path.join(__dirname, "../output/trade/all_trade_data.json");
const stockMapPath = path.join(__dirname, "../config/stock_trade_map.json");
const outDir = path.join(__dirname, "../output/stocks");

if (!fs.existsSync(allDataPath)) {
  throw new Error("전체 수출 데이터 파일이 없습니다: output/trade/all_trade_data.json");
}

if (!fs.existsSync(stockMapPath)) {
  throw new Error("종목 매핑 파일이 없습니다: config/stock_trade_map.json");
}

const allData = JSON.parse(fs.readFileSync(allDataPath, "utf-8"));
const stockMap = JSON.parse(fs.readFileSync(stockMapPath, "utf-8"));

const items = Array.isArray(allData) ? allData : (allData.items || []);

if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

for (const [stockSlug, stockInfo] of Object.entries(stockMap)) {
  let filtered = [];

  for (const filter of stockInfo.filters) {
    const matched = items.filter(row => {
      const hs = String(row.hsSgn || row.hs_code || "");
      const region = String(row.region_code || row.sidoCd || "");
      return hs === filter.hs_code && region === filter.region_code;
    });

    filtered.push(...matched);
  }

  filtered.sort((a, b) => String(a.yymm).localeCompare(String(b.yymm)));

  const latest = filtered.length ? filtered[filtered.length - 1] : null;
  const prev = filtered.length >= 2 ? filtered[filtered.length - 2] : null;

  const latestExport = latest ? Number(latest.export_usd || 0) : 0;
  const prevExport = prev ? Number(prev.export_usd || 0) : 0;

  const mom = prevExport ? ((latestExport - prevExport) / prevExport) * 100 : null;

  const result = {
    stock_slug: stockSlug,
    stock_name: stockInfo.name,
    filters: stockInfo.filters,
    updated_at: new Date().toISOString(),
    summary: {
      latest_yymm: latest ? latest.yymm : null,
      latest_export_usd: latestExport,
      mom: mom
    },
    series: filtered
  };

  fs.writeFileSync(
    path.join(outDir, `${stockSlug}-trade.json`),
    JSON.stringify(result, null, 2),
    "utf-8"
  );

  console.log(`생성 완료: ${stockSlug}-trade.json`);
}
