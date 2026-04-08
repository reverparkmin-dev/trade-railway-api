const express = require("express");
const cors = require("cors");
const { Client } = require("pg");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

function getPgClient() {
  return new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });
}

app.get("/", (req, res) => {
  res.json({ ok: true, message: "Railway trade API is running" });
});

app.get("/cache/meta/latest", async (req, res) => {
  const client = getPgClient();
  try {
    await client.connect();
    const result = await client.query(
      `SELECT value, updated_at FROM trade_meta WHERE key = 'latest' LIMIT 1`
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "cache not found" });
    }

    const data = result.rows[0].value;
    data.updated_at = result.rows[0].updated_at;

    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  } finally {
    await client.end();
  }
});

app.get("/cache/detail", async (req, res) => {
  const hsSgn = req.query.hsSgn || "854231";
  const sidoCd = req.query.sidoCd || "11";

  const client = getPgClient();
  try {
    await client.connect();
    const result = await client.query(
      `
      SELECT payload, updated_at
      FROM trade_cache_detail
      WHERE hs_code = $1 AND region_code = $2
      LIMIT 1
      `,
      [hsSgn, sidoCd]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "cache not found" });
    }

    const data = result.rows[0].payload;
    data.updated_at = result.rows[0].updated_at;

    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  } finally {
    await client.end();
  }
});

app.get("/cache/timeseries", async (req, res) => {
  const hsSgn = req.query.hsSgn || "854231";
  const sidoCd = req.query.sidoCd || "11";

  const client = getPgClient();
  try {
    await client.connect();
    const result = await client.query(
      `
      SELECT payload, updated_at
      FROM trade_cache_timeseries
      WHERE hs_code = $1 AND region_code = $2
      LIMIT 1
      `,
      [hsSgn, sidoCd]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "cache not found" });
    }

    const data = result.rows[0].payload;
    data.updated_at = result.rows[0].updated_at;

    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  } finally {
    await client.end();
  }
});

app.get("/cache/region", async (req, res) => {
  const hsSgn = req.query.hsSgn || "854231";

  const client = getPgClient();
  try {
    await client.connect();
    const result = await client.query(
      `
      SELECT payload, updated_at
      FROM trade_cache_region
      WHERE hs_code = $1
      LIMIT 1
      `,
      [hsSgn]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, message: "cache not found" });
    }

    const data = result.rows[0].payload;
    data.updated_at = result.rows[0].updated_at;

    res.json(data);
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  } finally {
    await client.end();
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
