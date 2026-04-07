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

// 숫자 정리
function cleanNumber(value) {
  if (value === undefined || value === null || value === "") return 0;
  return Number(String(value).replace(/,/g, "").trim()) || 0;
}

// 배열 변환
function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

// YYYYMM 목록 생성
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

// 응답 파싱
async function parseApiResponse(data) {
  try {
    return JSON.parse(data);
  } catch {
    return await parseStringPromise(data, { explicitArray: false });
  }
}

// 아이템 추출
function extractItems(parsed) {
  return asArray(parsed?.response?.body?.items?.item);
}

// 헤더 추출
function extractHeader(parsed) {
  const h = parsed?.response?.header || {};
  return {
    resultCode: h.resultCode || "",
    resultMsg: h.resultMsg || ""
  };
}

// 원본 아이템 매핑
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

// 단일 월 데이터 가져오기
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

// 서버 확인
app.get("/", (req, res) => {
  res.json({ ok: true, message: "Railway trade API is running" });
});

// 기존 상세 데이터 API
app.get("/api/export-data", async (req, res) => {
  try {
    const result = await fetchTradeMonth({
      yymm: req.query.strtYymm || "202501",
      hsSgn: req.query.hsSgn || "",
      sidoCd: req.query.sidoCd || ""
    });

    res.json({
      success: true,
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

// 신규 월별 시계열 API
app.get("/api/export-timeseries", async (req, res) => {
  try {
    const hsSgn = req.query.hsSgn || "";
    const sidoCd = req.query.sidoCd || "";
    const from = req.query.from || "202401";
    const to = req.query.to || "202503";

    const months = getMonthRange(from, to);
    const rows = [];

    for (const yymm of months) {
      const result = await fetchTradeMonth({ yymm, hsSgn, sidoCd });

      // 총계행 또는 첫 번째 유효행 우선 사용
      let target =
        result.items.find((item) => !item.hs_code) ||
        result.items[0] ||
        {
          period: yymm,
          hs_code: hsSgn,
          region_code: sidoCd,
          region_name: "",
          product_name: "",
          export_usd: 0,
          import_usd: 0,
          trade_balance_usd: 0,
          export_count: 0,
          import_count: 0
        };

      // 총계행이 없고 세부행 여러 개면 합산
      if (result.items.length > 1 && target.hs_code) {
        const summed = result.items.reduce(
          (acc, cur) => {
            acc.export_usd += cur.export_usd;
            acc.import_usd += cur.import_usd;
            acc.trade_balance_usd += cur.trade_balance_usd;
            acc.export_count += cur.export_count;
            acc.import_count += cur.import_count;
            if (!acc.product_name && cur.product_name) acc.product_name = cur.product_name;
            if (!acc.region_name && cur.region_name) acc.region_name = cur.region_name;
            return acc;
          },
          {
            product_name: "",
            region_name: "",
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
          region_name: summed.region_name,
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
        region_name: target.region_name || "",
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

// 서버 실행
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
