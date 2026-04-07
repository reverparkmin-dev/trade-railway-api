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

function cleanNumber(value) {
  if (value === undefined || value === null) return 0;
  const str = String(value).replace(/,/g, "").trim();
  const num = Number(str);
  return Number.isFinite(num) ? num : 0;
}

function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

async function parseApiResponse(rawText) {
  try {
    return JSON.parse(rawText);
  } catch (_) {}
  return await parseStringPromise(rawText, { explicitArray: false });
}

function extractItems(parsed) {
  if (parsed?.response?.body?.items?.item) {
    return asArray(parsed.response.body.items.item);
  }
  return [];
}

function extractHeader(parsed) {
  const header = parsed?.response?.header || {};
  return {
    resultCode: header.resultCode || "",
    resultMsg: header.resultMsg || ""
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
    export_count: cleanNumber(item.exp
