import fs from 'fs';
import dotenv from 'dotenv';
import { Pool } from 'pg';
import {
  AUTHORITY_HISTORY_BATCH_SIZE,
  buildAuthorityHistoryInsertBatch,
  mapAuthorityHistoryRow,
  parseCsvRecords,
  type AuthorityHistoryImportRow,
} from '../src/importers/authorityHistoryImporter';

dotenv.config({ quiet: true });

interface ImportStats {
  rowsRead: number;
  rowsAttempted: number;
  brokerAuthorityRows: number;
  carrierAuthorityRows: number;
  negativeFinalActionRows: number;
  batches: number;
}

async function insertBatch(pool: Pool, rows: AuthorityHistoryImportRow[], stats: ImportStats) {
  if (rows.length === 0) {
    return;
  }

  const batch = buildAuthorityHistoryInsertBatch(rows);
  await pool.query(batch.text, batch.values);
  stats.rowsAttempted += rows.length;
  stats.brokerAuthorityRows += rows.filter((row) => row.is_broker_authority).length;
  stats.carrierAuthorityRows += rows.filter((row) => row.is_carrier_authority).length;
  stats.negativeFinalActionRows += rows.filter((row) => row.is_negative_final_action).length;
  stats.batches += 1;
}

async function main() {
  const filePath = process.argv[2];

  if (!filePath) {
    throw new Error('Usage: npm run import:authority-history -- /path/to/AuthHist_All_With_History.txt');
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const startedAt = process.hrtime.bigint();
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const stats: ImportStats = {
    rowsRead: 0,
    rowsAttempted: 0,
    brokerAuthorityRows: 0,
    carrierAuthorityRows: 0,
    negativeFinalActionRows: 0,
    batches: 0,
  };
  const pendingRows: AuthorityHistoryImportRow[] = [];

  try {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });

    for await (const fields of parseCsvRecords(stream)) {
      stats.rowsRead += 1;
      const row = mapAuthorityHistoryRow(fields);

      if (!row) {
        continue;
      }

      pendingRows.push(row);

      if (pendingRows.length >= AUTHORITY_HISTORY_BATCH_SIZE) {
        await insertBatch(pool, pendingRows.splice(0), stats);
      }
    }

    await insertBatch(pool, pendingRows, stats);
  } finally {
    await pool.end();
  }

  const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

  console.log(`total rows read: ${stats.rowsRead}`);
  console.log(`total rows attempted: ${stats.rowsAttempted}`);
  console.log(`total broker authority rows: ${stats.brokerAuthorityRows}`);
  console.log(`total carrier authority rows: ${stats.carrierAuthorityRows}`);
  console.log(`total negative final action rows: ${stats.negativeFinalActionRows}`);
  console.log(`total batches: ${stats.batches}`);
  console.log(`import duration: ${(durationMs / 1000).toFixed(2)}s`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
