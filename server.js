const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { parseStringPromise } = require("xml2js");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

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

function extractHeader(parsed) {
  const h = parsed?.response?.header || {};
  return {
    resultCode: h.resultCode || "",
    resultMsg: h.resultMsg || ""
  };
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
    timeout: 20000,
    responseType: "text"
  });

  const parsed = await parseApiResponse(response.data);
  const header = extractHeader(parsed);
  const items = extractItems(parsed).map(mapItem);

  return {
    header,
    items
  };
}

function getCurrentYymm() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}${month}`;
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
    try {
      const result = await fetchTradeMonth({ yymm: current, hsSgn, sidoCd });
      const hasUsefulData = result.items.some(
        (item) =>
          item.export_usd > 0 ||
          item.import_usd > 0 ||
          item.trade_balance_usd !== 0 ||
          item.export_count > 0 ||
          item.import_count > 0
      );

      if (hasUsefulData) {
        return current;
      }
    } catch (e) {
      // 무시하고 이전 월로 이동
    }
    current = getPrevYymm(current);
  }

  return getPrevYymm(getCurrentYymm());
}

app.get("/", (req, res) => {
  res.json({ ok: true, message: "Railway trade API is running" });
});

app.get("/api/export-data", async (req, res) => {
  try {
    const hsSgn = req.query.hsSgn || "";
    const sidoCd = req.query.sidoCd || "";
    const latestYymm = await findLatestAvailableYymm({ hsSgn, sidoCd });

    const yymm = req.query.strtYymm || latestYymm;

    const result = await fetchTradeMonth({
      yymm,
      hsSgn,
      sidoCd
    });

    res.json({
      success: true,
      latest_yymm: latestYymm,
      resultCode: result.header.resultCode,
      resultMsg: result.header.resultMsg,
      count: result.items.length,
      items: result.items
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      message: e.message
    });
  }
});

app.get("/api/export-timeseries", async (req, res) => {
  try {
    const hsSgn = req.query.hsSgn || "";
    const sidoCd = req.query.sidoCd || "";
    const from = req.query.from || "202401";
    const latestYymm = await findLatestAvailableYymm({ hsSgn, sidoCd });
    const to = req.query.to || latestYymm;

    const months = getMonthRange(from, to);
    const rows = [];

    for (const yymm of months) {
      const result = await fetchTradeMonth({ yymm, hsSgn, sidoCd });

      let target =
        result.items.find((item) => !item.hs_code) ||
        result.items[0] ||
        {
          period: yymm,
          hs_code: hsSgn,
          region_code: sidoCd,
          region_name: REGION_NAME_MAP[sidoCd] || "",
          product_name: "",
          export_usd: 0,
          import_usd: 0,
          trade_balance_usd: 0,
          export_count: 0,
          import_count: 0
        };

      if (result.items.length > 1 && target.hs_code) {
        const summed = result.items.reduce(
          (acc, cur) => {
            acc.export_usd += cur.export_usd;
            acc.import_usd += cur.import_usd;
            acc.trade_balance_usd += cur.trade_balance_usd;
            acc.export_count += cur.export_count;
            acc.import_count += cur.import_count;
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

        target = {
          period: yymm,
          hs_code: hsSgn,
          region_code: sidoCd,
          region_name: REGION_NAME_MAP[sidoCd] || "",
          product_name: summed.product_name,
          export_usd: summed.export_usd,
          import_usd: summed.import_usd,
          trade_balance_usd: summed.trade_balance_usd,
          export_count: summed.export_count,
          import_count: summed.import_count
        };
      }

      rows.push({
        yymm,
        hs_code: hsSgn,
        region_code: sidoCd,
        region_name: target.region_name || REGION_NAME_MAP[sidoCd] || "",
        product_name: target.product_name || "",
        export_usd: target.export_usd || 0,
        import_usd: target.import_usd || 0,
        trade_balance_usd: target.trade_balance_usd || 0,
        export_count: target.export_count || 0,
        import_count: target.import_count || 0
      });
    }

    res.json({
      success: true,
      latest_yymm: latestYymm,
      hsSgn,
      sidoCd,
      from,
      to,
      count: rows.length,
      items: rows
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      message: e.message
    });
  }
});

app.get("/api/export-by-region", async (req, res) => {
  try {
    const hsSgn = req.query.hsSgn || "";
    const latestYymm = await findLatestAvailableYymm({ hsSgn });
    const yymm = req.query.yymm || latestYymm;

    const regionCodes = Object.keys(REGION_NAME_MAP);
    const rows = [];

    for (const code of regionCodes) {
      try {
        const result = await fetchTradeMonth({ yymm, hsSgn, sidoCd: code });

        let exportUsd = 0;
        let productName = "";

        const totalRow = result.items.find((item) => !item.hs_code);

        if (totalRow) {
          exportUsd = totalRow.export_usd || 0;
          productName = totalRow.product_name || "";
        } else if (result.items.length > 0) {
          exportUsd = result.items.reduce((sum, cur) => sum + (cur.export_usd || 0), 0);
          productName = result.items.find((x) => x.product_name)?.product_name || "";
        }

        rows.push({
          region_code: code,
          region_name: REGION_NAME_MAP[code] || code,
          hs_code: hsSgn,
          product_name: productName,
          export_usd: exportUsd
        });
      } catch (e) {
        rows.push({
          region_code: code,
          region_name: REGION_NAME_MAP[code] || code,
          hs_code: hsSgn,
          product_name: "",
          export_usd: 0
        });
      }
    }

    const filtered = rows
      .filter((row) => row.export_usd > 0)
      .sort((a, b) => b.export_usd - a.export_usd);

    const totalExportUsd = filtered.reduce((sum, cur) => sum + cur.export_usd, 0);

    const items = filtered.map((row) => ({
      ...row,
      share_pct: totalExportUsd > 0 ? Number(((row.export_usd / totalExportUsd) * 100).toFixed(2)) : 0
    }));

    res.json({
      success: true,
      latest_yymm: latestYymm,
      yymm,
      hsSgn,
      total_export_usd: totalExportUsd,
      count: items.length,
      items
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      message: e.message
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
