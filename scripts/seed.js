#!/usr/bin/env node
/**
 * Source-DB data generator. Three subcommands:
 *
 *   node seed.js bulk    --rows=2000000 [--batch=5000]
 *     One-shot bulk load, used before backfill starts.
 *
 *   node seed.js drip     --rate=20 --duration=120
 *     Continuously inserts/updates/soft-deletes existing rows at ~`rate`
 *     rows/sec for `duration` seconds (0 = forever). This is what the
 *     incremental sync worker has something to catch.
 *
 *   node seed.js corrupt  --count=3
 *     Writes `count` rows with a non-numeric `amount` ("N/A"), which the
 *     Elasticsearch sink's mapping will reject at bulk-item level. Used by
 *     verify.sh to drive gate G4 deterministically.
 */
const { Client } = require('pg');
const { CITY_COUNTRY_PAIRS } = require('./geo');

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/killittwice';

// Names stay a small generic placeholder pool (like "Jane Doe" — not
// sourced from or resembling any real individual's record). City/country
// come from real, public geographic reference data instead of a
// hand-picked list — see geo.js for why. Company names stay obviously
// fictional (the "Acme Corp" convention) rather than naming real
// businesses in synthetic "customer activity" records.
const FIRST = ['Nino', 'Giorgi', 'Ana', 'Luka', 'Mariam', 'Sandro', 'Tekla', 'Beka', 'Salome', 'Nika', 'Elene', 'Data', 'Levan', 'Tamar', 'Zura', 'Irakli'];
const LAST = ['Beridze', 'Kapanadze', 'Lomidze', 'Chkheidze', 'Gelashvili', 'Tsiklauri', 'Maisuradze', 'Kiknadze', 'Abashidze', 'Sharashenidze'];
const COMPANY = ['Vector Labs', 'Northpeak', 'BlueOrbit', 'Ferrum Systems', 'Kappa Works', 'Rustavi Forge', 'Delta Grove', 'Cobalt & Co', 'Skyline Data', 'Anchorpoint'];
const STATUS = ['active', 'inactive', 'pending'];
const TAGS = ['vip', 'trial', 'enterprise', 'churned', 'lead', 'partner', 'internal'];

function pick(arr) { return arr[(Math.random() * arr.length) | 0]; }
function pickSome(arr, max) {
  const n = 1 + ((Math.random() * max) | 0);
  const out = new Set();
  while (out.size < n) out.add(pick(arr));
  return [...out];
}
function randomAmount() { return (Math.random() * 10000).toFixed(2); }
function tagsLiteral(tags) { return `{${tags.join(',')}}`; }

function genRow(i) {
  const first = pick(FIRST);
  const last = pick(LAST);
  const place = pick(CITY_COUNTRY_PAIRS);
  return {
    name: `${first} ${last}`,
    email: `${first}.${last}.${i}@example.com`.toLowerCase(),
    company: pick(COMPANY),
    city: place.city,
    country: place.country,
    status: pick(STATUS),
    tags: pickSome(TAGS, 3),
    amount: randomAmount(),
  };
}

function parseArgs(argv) {
  const out = {};
  for (const a of argv) {
    const m = a.match(/^--([^=]+)=(.*)$/);
    if (m) out[m[1]] = m[2];
  }
  return out;
}

async function bulk(client, args) {
  const rows = parseInt(args.rows || '2000000', 10);
  const batchSize = parseInt(args.batch || '5000', 10);
  console.log(`[seed] bulk loading ${rows} rows in batches of ${batchSize}...`);
  const start = Date.now();
  let inserted = 0;

  for (let offset = 0; offset < rows; offset += batchSize) {
    const n = Math.min(batchSize, rows - offset);
    const names = [], emails = [], companies = [], cities = [], countries = [], statuses = [], tags = [], amounts = [];
    for (let i = 0; i < n; i++) {
      const r = genRow(offset + i);
      names.push(r.name); emails.push(r.email); companies.push(r.company);
      cities.push(r.city); countries.push(r.country); statuses.push(r.status);
      tags.push(tagsLiteral(r.tags)); amounts.push(r.amount);
    }
    // tags travels as a flat text[] of array-literal strings ("{vip,lead}")
    // and gets cast to text[] per-row in the SELECT — UNNEST on a genuine
    // text[][] parameter flattens both dimensions instead of zipping one
    // array per output row, which silently produces the wrong shape.
    await client.query(
      `INSERT INTO records (name, email, company, city, country, status, tags, amount)
       SELECT n, e, c, ci, co, s, tl::text[], a
       FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[])
         AS t(n, e, c, ci, co, s, tl, a)`,
      [names, emails, companies, cities, countries, statuses, tags, amounts]
    );
    inserted += n;
    if (inserted % (batchSize * 20) === 0 || inserted === rows) {
      const rate = (inserted / ((Date.now() - start) / 1000)).toFixed(0);
      console.log(`[seed] ${inserted}/${rows} (${rate} rows/sec)`);
    }
  }
  console.log(`[seed] done: ${inserted} rows in ${((Date.now() - start) / 1000).toFixed(1)}s`);
}

async function drip(client, args) {
  const rate = parseFloat(args.rate || '20');
  const durationSec = parseInt(args.duration || '0', 10);
  const intervalMs = Math.max(50, 1000 / rate);
  console.log(`[seed] drip: ~${rate} changes/sec, duration=${durationSec || 'forever'}s`);
  const start = Date.now();
  let n = 0;

  while (durationSec === 0 || (Date.now() - start) / 1000 < durationSec) {
    const op = Math.random();
    if (op < 0.5) {
      // insert
      const r = genRow(`drip-${Date.now()}-${n}`);
      await client.query(
        `INSERT INTO records (name, email, company, city, country, status, tags, amount)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [r.name, r.email, r.company, r.city, r.country, r.status, r.tags, r.amount]
      );
    } else if (op < 0.9) {
      // update a random existing row (bumps updated_at + version)
      await client.query(
        `UPDATE records SET status = $1, amount = $2, version = version + 1, updated_at = now()
         WHERE id = (SELECT id FROM records WHERE deleted_at IS NULL ORDER BY random() LIMIT 1)`,
        [pick(STATUS), randomAmount()]
      );
    } else {
      // soft delete a random existing row
      await client.query(
        `UPDATE records SET deleted_at = now(), updated_at = now(), version = version + 1
         WHERE id = (SELECT id FROM records WHERE deleted_at IS NULL ORDER BY random() LIMIT 1)`
      );
    }
    n++;
    await new Promise((res) => setTimeout(res, intervalMs));
  }
  console.log(`[seed] drip done: ${n} changes applied`);
}

async function corrupt(client, args) {
  const count = parseInt(args.count || '3', 10);
  for (let i = 0; i < count; i++) {
    const r = genRow(`corrupt-${Date.now()}-${i}`);
    await client.query(
      `INSERT INTO records (name, email, company, city, country, status, tags, amount)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'N/A')`,
      [r.name, r.email, r.company, r.city, r.country, r.status, r.tags]
    );
  }
  console.log(`[seed] inserted ${count} corrupt row(s) (amount='N/A')`);
}

async function main() {
  const [, , cmd, ...rest] = process.argv;
  const args = parseArgs(rest);
  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();
  try {
    if (cmd === 'bulk') await bulk(client, args);
    else if (cmd === 'drip') await drip(client, args);
    else if (cmd === 'corrupt') await corrupt(client, args);
    else {
      console.error('usage: seed.js <bulk|drip|corrupt> [--flag=value ...]');
      process.exit(1);
    }
  } finally {
    await client.end();
  }
}

main().catch((err) => { console.error(err); process.exit(1); });
