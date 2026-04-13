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
  "300490",
  "852351",
  "300249",
  "854129"
];

const DEFAULT_FROM_YYMM = process.env.TRADE_FROM_YYMM || "202401";
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 120);
const HS_DELAY_MS = Number(process.env.HS_DELAY_MS || 500);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);

function cleanNumber(value) {
  if (value === undefined || value === null || value === "") return 0;
  return Number(String(value).replace(/,/g, "").trim()) || 0;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options, maxRetries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await axios.get(url, options);
    } catch (err) {
      const status = err?.response?.status;

      if (status === 429 && attempt < maxRetries) {
        const waitMs = 1500 * (attempt + 1);
        console.warn(`[429] retry after ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }

      if (!status && attempt < maxRetries) {
        const waitMs = 1500 * (attempt + 1);
        console.warn(`[network] retry after ${waitMs}ms: ${err.message}`);
        await sleep(waitMs);
        continue;
      }

      throw err;
    }
  }
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

    await sleep(REQUEST_DELAY_MS);

    const response = await fetchWithRetry(BASE_URL, {
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

async function findLatestAvailableYymm({ hsSgn = "", maxLookback = 12 }) {
  let current = getCurrentYymm();

  for (let i = 0; i < maxLookback; i++) {
    let hasUsefulData = false;

    for (const regionCode of CORE_REGION_CODES) {
      const items = await fetchTradeMonth({ yymm: current, hsSgn, sidoCd: regionCode });
      const target =
        items.find((item) => !item.hs_code) ||
        items[0] || {
          export_usd: 0,
          import_usd: 0,
          trade_balance_usd: 0,
          export_count: 0,
          import_count: 0
        };

      if (
        target.export_usd > 0 ||
        target.import_usd > 0 ||
        target.trade_balance_usd !== 0 ||
        target.export_count > 0 ||
        target.import_count > 0
      ) {
        hasUsefulData = true;
        break;
      }
    }

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

function createEmptySummary(hsCode, regionCode, yymm, productName = "") {
  return {
    yymm,
    hs_code: hsCode,
    region_code: regionCode,
    region_name: regionCode === "ALL" ? "전체" : (REGION_NAME_MAP[regionCode] || regionCode),
    product_name: productName,
    export_usd: 0,
    import_usd: 0,
    trade_balance_usd: 0,
    export_count: 0,
    import_count: 0
  };
}

async function collectHsMonthRegionData(hsSgn, months) {
  const matrix = {};
  let productNameFallback = "";

  for (const yymm of months) {
    matrix[yymm] = {};

    for (const regionCode of CORE_REGION_CODES) {
      const items = await fetchTradeMonth({ yymm, hsSgn, sidoCd: regionCode });
      const target = pickTotalLikeRow(items);

      if (!productNameFallback && target.product_name) {
        productNameFallback = target.product_name;
      }

      matrix[yymm][regionCode] = {
        summary: {
          yymm,
          hs_code: hsSgn,
          region_code: regionCode,
          region_name: REGION_NAME_MAP[regionCode] || regionCode,
          product_name: target.product_name || productNameFallback || "",
          export_usd: target.export_usd || 0,
          import_usd: target.import_usd || 0,
          trade_balance_usd: target.trade_balance_usd || 0,
          export_count: target.export_count || 0,
          import_count: target.import_count || 0
        },
        detailItems: items
      };
    }
  }

  return {
    matrix,
    productNameFallback
  };
}

async function buildOneHs(client, hsSgn, from = DEFAULT_FROM_YYMM) {
  const latestYymm = await findLatestAvailableYymm({ hsSgn });
  const months = getMonthRange(from, latestYymm);

  const { matrix, productNameFallback } = await collectHsMonthRegionData(hsSgn, months);

  const latestRegionRows = CORE_REGION_CODES.map((regionCode) => {
    const row = matrix[latestYymm]?.[regionCode]?.summary || createEmptySummary(hsSgn, regionCode, latestYymm, productNameFallback);

    return {
      region_code: regionCode,
      region_name: REGION_NAME_MAP[regionCode] || regionCode,
      hs_code: hsSgn,
      product_name: row.product_name || productNameFallback || "",
      export_usd: row.export_usd || 0
    };
  });

  const totalAllLatest = latestRegionRows.reduce((sum, cur) => sum + (cur.export_usd || 0), 0);

  const regionRows = [
    {
      region_code: "ALL",
      region_name: "전체",
      hs_code: hsSgn,
      product_name: productNameFallback || "",
      export_usd: totalAllLatest
    },
    ...latestRegionRows
  ];

  const regionFiltered = regionRows
    .filter((row) => row.export_usd > 0)
    .sort((a, b) => {
      if (a.region_code === "ALL") return -1;
      if (b.region_code === "ALL") return 1;
      return b.export_usd - a.export_usd;
    });

  const totalExportUsd = latestRegionRows.reduce((sum, cur) => sum + (cur.export_usd || 0), 0);

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

  for (const regionCode of CORE_REGION_CODES) {
    const timeseries = months.map((yymm) => {
      const row = matrix[yymm]?.[regionCode]?.summary || createEmptySummary(hsSgn, regionCode, yymm, productNameFallback);
      return {
        ...row,
        product_name: row.product_name || productNameFallback || ""
      };
    });

    const latestDetailItems = matrix[latestYymm]?.[regionCode]?.detailItems || [];

    const timeseriesPayload = {
      success: true,
      latest_yymm: latestYymm,
      hsSgn,
      sidoCd: regionCode,
      from,
      to: latestYymm,
      count: timeseries.length,
      items: timeseries
    };

    const detailPayload = {
      success: true,
      latest_yymm: latestYymm,
      hsSgn,
      sidoCd: regionCode,
      count: latestDetailItems.length,
      items: latestDetailItems
    };

    await upsertTimeseries(client, hsSgn, regionCode, latestYymm, timeseriesPayload);
    await upsertDetail(client, hsSgn, regionCode, latestYymm, detailPayload);
  }

  const allTimeseries = months.map((yymm) => {
    const summary = createEmptySummary(hsSgn, "ALL", yymm, productNameFallback);

    for (const regionCode of CORE_REGION_CODES) {
      const row = matrix[yymm]?.[regionCode]?.summary || createEmptySummary(hsSgn, regionCode, yymm, productNameFallback);
      summary.export_usd += row.export_usd || 0;
      summary.import_usd += row.import_usd || 0;
      summary.trade_balance_usd += row.trade_balance_usd || 0;
      summary.export_count += row.export_count || 0;
      summary.import_count += row.import_count || 0;
      if (!summary.product_name && row.product_name) summary.product_name = row.product_name;
    }

    return summary;
  });

  const allLatestDetailItems = CORE_REGION_CODES.map((regionCode) => {
    const row = matrix[latestYymm]?.[regionCode]?.summary || createEmptySummary(hsSgn, regionCode, latestYymm, productNameFallback);

    return {
      period: latestYymm,
      region_code: regionCode,
      region_name: REGION_NAME_MAP[regionCode] || regionCode,
      hs_code: hsSgn,
      product_name: row.product_name || productNameFallback || "",
      export_usd: row.export_usd || 0,
      import_usd: row.import_usd || 0,
      trade_balance_usd: row.trade_balance_usd || 0,
      export_count: row.export_count || 0,
      import_count: row.import_count || 0
    };
  });

  const allTimeseriesPayload = {
    success: true,
    latest_yymm: latestYymm,
    hsSgn,
    sidoCd: "ALL",
    from,
    to: latestYymm,
    count: allTimeseries.length,
    items: allTimeseries
  };

  const allDetailPayload = {
    success: true,
    latest_yymm: latestYymm,
    hsSgn,
    sidoCd: "ALL",
    count: allLatestDetailItems.length,
    items: allLatestDetailItems
  };

  await upsertTimeseries(client, hsSgn, "ALL", latestYymm, allTimeseriesPayload);
  await upsertDetail(client, hsSgn, "ALL", latestYymm, allDetailPayload);

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
        const result = await buildOneHs(client, hs, DEFAULT_FROM_YYMM);
        results.push(result);
        console.log(`✅ Completed ${hs}`);
      } catch (err) {
        console.error(`❌ ERROR on HS ${hs}:`, err.message);
      }

      await sleep(HS_DELAY_MS);
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
