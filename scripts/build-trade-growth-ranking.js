const fs = require("fs");
const path = require("path");
const axios = require("axios");
const { parseStringPromise } = require("xml2js");
require("dotenv").config();

const BASE_URL =
  process.env.CUSTOMS_API_URL ||
  "https://apis.data.go.kr/1220000/sidoitemtrade/getSidoitemtradeList";

const SERVICE_KEY =
  process.env.CUSTOMS_API_KEY ||
  process.env.PUBLIC_API_SERVICE_KEY ||
  process.env.SERVICE_KEY;

const DATA_DIR = path.join(__dirname, "..", "data");
const CONFIG_FILE = path.join(__dirname, "..", "config", "stock_trade_map.json");
const OUTPUT_FILE = path.join(DATA_DIR, "trade-growth-ranking.json");

const MIN_AMOUNT_USD = 10000000;
const TOP_LIMIT = 5;

const REGION_CODES = [
  "11", "26", "27", "28", "29", "30", "31", "36",
  "41", "42", "43", "44", "45", "46", "47", "48", "50"
];

function ensureDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function readHsCodes() {
  if (!fs.existsSync(CONFIG_FILE)) {
    throw new Error("config/stock_trade_map.json 파일이 없습니다.");
  }

  const raw = fs.readFileSync(CONFIG_FILE, "utf8");
  const json = JSON.parse(raw);

  const codes = new Set();

  const scan = value => {
    if (!value) return;

    if (Array.isArray(value)) {
      value.forEach(scan);
      return;
    }

    if (typeof value === "object") {
      const possible =
        value.hs_code ||
        value.hsCode ||
        value.hsSgn ||
        value.hs ||
        value.code;

      if (possible && String(possible).match(/^\d{6}$/)) {
        codes.add(String(possible));
      }

      Object.values(value).forEach(scan);
    }
  };

  scan(json);

  return [...codes];
}

function getRecentYmList(count = 18) {
  const result = [];
  const now = new Date();

  for (let i = 1; i <= count; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    result.push(`${y}${m}`);
  }

  return result;
}

function prevMonth(yymm) {
  const y = Number(yymm.slice(0, 4));
  const m = Number(yymm.slice(4, 6));
  const d = new Date(y, m - 2, 1);
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}`;
}

function prevYear(yymm) {
  return `${Number(yymm.slice(0, 4)) - 1}${yymm.slice(4, 6)}`;
}

function toNumber(v) {
  if (v === null || v === undefined || v === "") return 0;
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function rate(current, base) {
  if (!base || base <= 0) return null;
  return ((current - base) / base) * 100;
}

async function fetchOne({ yymm, hsCode, sidoCd }) {
  const params = {
    serviceKey: SERVICE_KEY,
    pageNo: 1,
    numOfRows: 100,
    basYm: yymm,
    hsSgn: hsCode,
    sidoCd
  };

  const res = await axios.get(BASE_URL, {
    params,
    timeout: 20000
  });

  const text = typeof res.data === "string" ? res.data : JSON.stringify(res.data);

  let items = [];

  if (typeof res.data === "object") {
    items =
      res.data?.response?.body?.items?.item ||
      res.data?.items ||
      [];
  } else {
    const parsed = await parseStringPromise(text, {
      explicitArray: false,
      trim: true
    });

    items =
      parsed?.response?.body?.items?.item ||
      parsed?.items?.item ||
      [];
  }

  if (!Array.isArray(items)) items = items ? [items] : [];

  return items.reduce((sum, item) => {
    return sum + toNumber(
      item.expDlr ||
      item.expAmt ||
      item.exportAmount ||
      item.EXP_DLR
    );
  }, 0);
}

async function fetchTotalExport({ yymm, hsCode }) {
  let total = 0;

  for (const sidoCd of REGION_CODES) {
    try {
      const amount = await fetchOne({ yymm, hsCode, sidoCd });
      total += amount;
      await new Promise(resolve => setTimeout(resolve, 120));
    } catch (err) {
      console.error(`[WARN] ${yymm} ${hsCode} ${sidoCd} 조회 실패:`, err.message);
    }
  }

  return total;
}

async function build() {
  if (!SERVICE_KEY) {
    throw new Error("CUSTOMS_API_KEY 또는 PUBLIC_API_SERVICE_KEY 환경변수가 필요합니다.");
  }

  ensureDir();

  const hsCodes = readHsCodes();

  if (!hsCodes.length) {
    throw new Error("stock_trade_map.json에서 HS코드를 찾지 못했습니다.");
  }

  console.log(`[START] HS ${hsCodes.length}개 기준 수출 증가율 순위 생성`);

  const ymCandidates = getRecentYmList(6);
  let latestYymm = null;
  let latestTotal = 0;

  for (const yymm of ymCandidates) {
    let sum = 0;

    for (const hsCode of hsCodes.slice(0, 5)) {
      sum += await fetchTotalExport({ yymm, hsCode });
    }

    if (sum > 0) {
      latestYymm = yymm;
      latestTotal = sum;
      break;
    }
  }

  if (!latestYymm) {
    throw new Error("최신 수출 데이터가 있는 기준월을 찾지 못했습니다.");
  }

  const momYymm = prevMonth(latestYymm);
  const yoyYymm = prevYear(latestYymm);

  console.log(`[INFO] latest=${latestYymm}, mom=${momYymm}, yoy=${yoyYymm}`);

  const rows = [];

  for (const hsCode of hsCodes) {
    console.log(`[FETCH] HS ${hsCode}`);

    const currentAmount = await fetchTotalExport({ yymm: latestYymm, hsCode });
    const prevMonthAmount = await fetchTotalExport({ yymm: momYymm, hsCode });
    const prevYearAmount = await fetchTotalExport({ yymm: yoyYymm, hsCode });

    if (currentAmount < MIN_AMOUNT_USD) continue;

    const momRate = rate(currentAmount, prevMonthAmount);
    const yoyRate = rate(currentAmount, prevYearAmount);

    rows.push({
      hs_code: hsCode,
      hs_name: "",
      current_amount: currentAmount,
      prev_month_amount: prevMonthAmount,
      prev_year_amount: prevYearAmount,
      mom_rate: momRate,
      yoy_rate: yoyRate
    });
  }

  const yoyTop5 = rows
    .filter(r => r.yoy_rate !== null)
    .sort((a, b) => b.yoy_rate - a.yoy_rate)
    .slice(0, TOP_LIMIT);

  const momTop5 = rows
    .filter(r => r.mom_rate !== null)
    .sort((a, b) => b.mom_rate - a.mom_rate)
    .slice(0, TOP_LIMIT);

  const output = {
    ok: true,
    latest_yymm: latestYymm,
    min_amount_usd: MIN_AMOUNT_USD,
    generated_at: new Date().toISOString(),
    yoy_top5: yoyTop5,
    mom_top5: momTop5
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2), "utf8");

  console.log(`[DONE] 저장 완료: ${OUTPUT_FILE}`);
}

build().catch(err => {
  console.error("[ERROR]", err);
  process.exit(1);
});
