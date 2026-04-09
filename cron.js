const axios = require("axios");
const { parseStringPromise } = require("xml2js");
const { Client } = require("pg");
require("dotenv").config();

const BASE_URL =
  process.env.CUSTOMS_API_URL ||
  "https://apis.data.go.kr/1220000/sidoitemtrade/getSidoitemtradeList";

const REGION_NAME_MAP = {
  "11": "서울",
  "26": "부산",
  "27": "대구",
  "28": "인천",
  "29": "광주",
  "30": "대전",
  "31": "울산",
  "36": "세종",
  "41": "경기",
  "42": "강원",
  "43": "충북",
  "44": "충남",
  "45": "전북",
  "46": "전남",
  "47": "경북",
  "48": "경남",
  "50": "제주"
};

const CORE_REGION_CODES = ["11", "28", "31", "41", "43", "44", "47", "48"];

const HS_LIST = [
  "854231",
  "854232",
  "854233",
  "854239",
  "854290",
  "847330",
  "848210",
  "850440",
  "852990",
  "853400",
  "854110",
  "854121",
  "842139",
  "854129"
];

function cleanNumber(value) {
  if (value === undefined || value === null || value === "") return 0;
  return Number(String(value).replace(/,/g, "").trim()) || 0;
}

function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function getMonthRange(from, to) {
  const result = [];
  let y = Number(String(from).slice(0, 4));
  let m = Number(String(from).slice(4, 6));
  const endY = Number(String(to).slice(0, 4));
  const endM = Number(String(to).slice(4, 6));

  while (y < endY || (y === endY && m <= endM)) {
    result.push(`${y}${String(m).padStart(2, "0")}`);
    m += 1;
    if (m > 12) {
      m = 1;
      y += 1;
    }
  }
  return result;
}

async function parseApiResponse(data) {
  try {
    return JSON.parse(data);
  } catch {
    return await parseStringPromise(data, { explicitArray: false });
  }
}

function extractItems(parsed) {
  return asArray(parsed?.response?.body?.items?.item);
}

function mapItem(item) {
  return {
    period: item.priodTitle || "",
    region_code: item.sidoCd || "",
    region_name: item.sidoNm || "",
    hs_code: item.hsSgn || "",
    product_name: item.korePrlstNm || "",
    export_usd: cleanNumber(item.expUsdAmt),
    import_usd: cleanNumber(item.impUsdAmt),
    trade_balance_usd: cleanNumber(item.cmtrBlncAmt),
    export_count: cleanNumber(item.expLnCnt),
    import_count: cleanNumber(item.impLnCnt)
  };
}

async function fetchTradeMonth({ yymm, hsSgn = "", sidoCd = "" }) {
  try {
    const serviceKey = process.env.CUSTOMS_SERVICE_KEY;

    const params = {
      serviceKey,
      strtYymm: yymm,
      endYymm: yymm,
      numOfRows: "1000",
      pageNo: "1"
    };

    if (hsSgn) params.hsSgn = hsSgn;
    if (sidoCd) params.sidoCd = sidoCd;

    const response = await axios.get(BASE_URL, {
      params,
      timeout: 30000,
      responseType: "text"
    });

    const parsed = await parseApiResponse(response.data);
    return extractItems(parsed).map(mapItem);
  } catch (err) {
    console.error(
      `[fetchTradeMonth] failed yymm=${yymm} hsSgn=${hsSgn} sidoCd=${sidoCd}: ${err.message}`
    );
    return [];
  }
}

function getCurrentYymm() {
  const now = new Date();
  return `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
}

function getPrevYymm(yymm) {
  let y = Number(String(yymm).slice(0, 4));
  let m = Number(String(yymm).slice(4, 6));
  m -= 1;
  if (m < 1) {
    m = 12;
    y -= 1;
  }
  return `${y}${String(m).padStart(2, "0")}`;
}

async function findLatestAvailableYymm({ hsSgn = "", sidoCd = "", maxLookback = 12 }) {
  let current = getCurrentYymm();

  for (let i = 0; i < maxLookback; i++) {
    const items = await fetchTradeMonth({ yymm: current, hsSgn, sidoCd });
    const hasUsefulData = items.some(
      (item) =>
        item.export_usd > 0 ||
        item.import_usd > 0 ||
        item.trade_balance_usd !== 0 ||
        item.export_count > 0 ||
        item.import_count > 0
    );
    if (hasUsefulData) return current;

    current = getPrevYymm(current);
  }

  return getPrevYymm(getCurrentYymm());
}

async function createTables(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS trade_meta (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS trade_cache_detail (
      hs_code TEXT NOT NULL,
      region_code TEXT NOT NULL,
      latest_yymm TEXT NOT NULL,
      payload JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (hs_code, region_code)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS trade_cache_timeseries (
      hs_code TEXT NOT NULL,
      region_code TEXT NOT NULL,
      latest_yymm TEXT NOT NULL,
      payload JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW(),
      PRIMARY KEY (hs_code, region_code)
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS trade_cache_region (
      hs_code TEXT PRIMARY KEY,
      latest_yymm TEXT NOT NULL,
      payload JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `);
}

async function upsertMeta(client, key, value) {
  await client.query(
    `
    INSERT INTO trade_meta (key, value, updated_at)
    VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    `,
    [key, JSON.stringify(value)]
  );
}

async function upsertDetail(client, hsCode, regionCode, latestYymm, payload) {
  await client.query(
    `
    INSERT INTO trade_cache_detail (hs_code, region_code, latest_yymm, payload, updated_at)
    VALUES ($1, $2, $3, $4::jsonb, NOW())
    ON CONFLICT (hs_code, region_code)
    DO UPDATE SET latest_yymm = EXCLUDED.latest_yymm, payload = EXCLUDED.payload, updated_at = NOW()
    `,
    [hsCode, regionCode, latestYymm, JSON.stringify(payload)]
  );
}

async function upsertTimeseries(client, hsCode, regionCode, latestYymm, payload) {
  await client.query(
    `
    INSERT INTO trade_cache_timeseries (hs_code, region_code, latest_yymm, payload, updated_at)
    VALUES ($1, $2, $3, $4::jsonb, NOW())
    ON CONFLICT (hs_code, region_code)
    DO UPDATE SET latest_yymm = EXCLUDED.latest_yymm, payload = EXCLUDED.payload, updated_at = NOW()
    `,
    [hsCode, regionCode, latestYymm, JSON.stringify(payload)]
  );
}

async function upsertRegion(client, hsCode, latestYymm, payload) {
  await client.query(
    `
    INSERT INTO trade_cache_region (hs_code, latest_yymm, payload, updated_at)
    VALUES ($1, $2, $3::jsonb, NOW())
    ON CONFLICT (hs_code)
    DO UPDATE SET latest_yymm = EXCLUDED.latest_yymm, payload = EXCLUDED.payload, updated_at = NOW()
    `,
    [hsCode, latestYymm, JSON.stringify(payload)]
  );
}

function pickTotalLikeRow(items) {
  return (
    items.find((item) => !item.hs_code) ||
    items[0] || {
      export_usd: 0,
      import_usd: 0,
      trade_balance_usd: 0,
      export_count: 0,
      import_count: 0,
      product_name: ""
    }
  );
}

async function fetchAllRegionsMonthSummary(yymm, hsSgn) {
  const regionSummaries = [];

  for (const regionCode of CORE_REGION_CODES) {
    const items = await fetchTradeMonth({ yymm, hsSgn, sidoCd: regionCode });
    const target = pickTotalLikeRow(items);

    regionSummaries.push({
      region_code: regionCode,
      region_name: REGION_NAME_MAP[regionCode] || regionCode,
      hs_code: hsSgn,
      product_name: target.product_name || "",
      export_usd: target.export_usd || 0,
      import_usd: target.import_usd || 0,
      trade_balance_usd: target.trade_balance_usd || 0,
      export_count: target.export_count || 0,
      import_count: target.import_count || 0
    });
  }

  const total = regionSummaries.reduce(
    (acc, cur) => {
      acc.export_usd += cur.export_usd || 0;
      acc.import_usd += cur.import_usd || 0;
      acc.trade_balance_usd += cur.trade_balance_usd || 0;
      acc.export_count += cur.export_count || 0;
      acc.import_count += cur.import_count || 0;
      if (!acc.product_name && cur.product_name) acc.product_name = cur.product_name;
      return acc;
    },
    {
      product_name: "",
      export_usd: 0,
      import_usd: 0,
      trade_balance_usd: 0,
      export_count: 0,
      import_count: 0
    }
  );

  return {
    summary: total,
    regions: regionSummaries
  };
}

async function buildOneHs(client, hsSgn, from = "202401") {
  const latestYymm = await findLatestAvailableYymm({ hsSgn });

  const regionRows = [];

  for (const code of CORE_REGION_CODES) {
    const items = await fetchTradeMonth({ yymm: latestYymm, hsSgn, sidoCd: code });

    let exportUsd = 0;
    let productName = "";

    const totalRow = items.find((item) => !item.hs_code);

    if (totalRow) {
      exportUsd = totalRow.export_usd || 0;
      productName = totalRow.product_name || "";
    } else if (items.length > 0) {
      exportUsd = items.reduce((sum, cur) => sum + (cur.export_usd || 0), 0);
      productName = items.find((x) => x.product_name)?.product_name || "";
    }

    regionRows.push({
      region_code: code,
      region_name: REGION_NAME_MAP[code] || code,
      hs_code: hsSgn,
      product_name: productName,
      export_usd: exportUsd
    });
  }

  const totalAll = regionRows.reduce((sum, cur) => sum + (cur.export_usd || 0), 0);

  regionRows.unshift({
    region_code: "ALL",
    region_name: "전체",
    hs_code: hsSgn,
    product_name: regionRows.find((row) => row.product_name)?.product_name || "",
    export_usd: totalAll
  });

  const regionFiltered = regionRows
    .filter((row) => row.export_usd > 0)
    .sort((a, b) => {
      if (a.region_code === "ALL") return -1;
      if (b.region_code === "ALL") return 1;
      return b.export_usd - a.export_usd;
    });

  const totalExportUsd = regionFiltered
    .filter((row) => row.region_code !== "ALL")
    .reduce((sum, cur) => sum + cur.export_usd, 0);

  const regionPayload = {
    success: true,
    latest_yymm: latestYymm,
    hsSgn,
    total_export_usd: totalExportUsd,
    count: regionFiltered.length,
    items: regionFiltered.map((row) => ({
      ...row,
      share_pct:
        row.region_code === "ALL"
          ? 100
          : totalExportUsd > 0
            ? Number(((row.export_usd / totalExportUsd) * 100).toFixed(2))
            : 0
    }))
  };

  await upsertRegion(client, hsSgn, latestYymm, regionPayload);

  for (const code of ["ALL", ...CORE_REGION_CODES]) {
    const months = getMonthRange(from, latestYymm);
    const timeseries = [];
    let latestDetailItems = [];

    for (const yymm of months) {
      if (code === "ALL") {
        const allData = await fetchAllRegionsMonthSummary(yymm, hsSgn);
        const target = allData.summary;

        if (yymm === latestYymm) {
          latestDetailItems = allData.regions.map((row) => ({
            period: yymm,
            region_code: row.region_code,
            region_name: row.region_name,
            hs_code: hsSgn,
            product_name: row.product_name || "",
            export_usd: row.export_usd || 0,
            import_usd: row.import_usd || 0,
            trade_balance_usd: row.trade_balance_usd || 0,
            export_count: row.export_count || 0,
            import_count: row.import_count || 0
          }));
        }

        timeseries.push({
          yymm,
          hs_code: hsSgn,
          region_code: "ALL",
          region_name: "전체",
          product_name: target.product_name || "",
          export_usd: target.export_usd || 0,
          import_usd: target.import_usd || 0,
          trade_balance_usd: target.trade_balance_usd || 0,
          export_count: target.export_count || 0,
          import_count: target.import_count || 0
        });
      } else {
        const items = await fetchTradeMonth({ yymm, hsSgn, sidoCd: code });
        const target = pickTotalLikeRow(items);

        if (yymm === latestYymm) {
          latestDetailItems = items;
        }

        timeseries.push({
          yymm,
          hs_code: hsSgn,
          region_code: code,
          region_name: REGION_NAME_MAP[code] || code,
          product_name: target.product_name || "",
          export_usd: target.export_usd || 0,
          import_usd: target.import_usd || 0,
          trade_balance_usd: target.trade_balance_usd || 0,
          export_count: target.export_count || 0,
          import_count: target.import_count || 0
        });
      }
    }

    const timeseriesPayload = {
      success: true,
      latest_yymm: latestYymm,
      hsSgn,
      sidoCd: code,
      from,
      to: latestYymm,
      count: timeseries.length,
      items: timeseries
    };

    const detailPayload = {
      success: true,
      latest_yymm: latestYymm,
      hsSgn,
      sidoCd: code === "ALL" ? "ALL" : code,
      count: latestDetailItems.length,
      items: latestDetailItems
    };

    await upsertTimeseries(client, hsSgn, code, latestYymm, timeseriesPayload);
    await upsertDetail(client, hsSgn, code, latestYymm, detailPayload);
  }

  return {
    hsSgn,
    latest_yymm: latestYymm
  };
}

async function main() {
  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();
    await createTables(client);

    const results = [];

    for (const hs of HS_LIST) {
      console.log(`Building cache for ${hs}...`);

      try {
        const result = await buildOneHs(client, hs, "202401");
        results.push(result);
        console.log(`✅ Completed ${hs}`);
      } catch (err) {
        console.error(`❌ ERROR on HS ${hs}:`, err.message);
      }
    }

    await upsertMeta(client, "latest", {
      success: true,
      generated_at: new Date().toISOString(),
      items: results
    });

    console.log("Cron build completed.");
  } finally {
    await client.end().catch(() => {});
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Cron build failed:", err);
    process.exit(1);
  });
