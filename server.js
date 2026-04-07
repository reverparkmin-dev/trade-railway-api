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
  "https://apis.data.go.kr/1220000/sidoitemtrade/getSidoItemTrade";

// 숫자 정리
function cleanNumber(value) {
  if (!value) return 0;
  return Number(String(value).replace(/,/g, "").trim()) || 0;
}

// 배열 변환
function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

// 응답 파싱 (JSON → XML 순)
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

// 데이터 매핑
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

// 루트 확인
app.get("/", (req, res) => {
  res.json({ ok: true, message: "Railway trade API is running" });
});

// 메인 API
app.get("/api/export-data", async (req, res) => {
  try {
    const serviceKey = process.env.CUSTOMS_SERVICE_KEY;

    const params = {
      serviceKey,
      strtYymm: req.query.strtYymm || "202501",
      endYymm: req.query.endYymm || "202501",
      sidoCd: req.query.sidoCd || "",
      hsSgn: req.query.hsSgn || "",
      numOfRows: "1000",
      pageNo: "1"
    };

    const response = await axios.get(BASE_URL, {
      params,
      timeout: 20000,
      responseType: "text"
    });

    const parsed = await parseApiResponse(response.data);
    const header = extractHeader(parsed);
    const items = extractItems(parsed).map(mapItem);

    res.json({
      success: true,
      resultCode: header.resultCode,
      resultMsg: header.resultMsg,
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

// 서버 실행 (중요)
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
