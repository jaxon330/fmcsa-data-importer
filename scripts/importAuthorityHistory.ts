import dotenv from 'dotenv';
import { Pool } from 'pg';
import {
  getBatchSize,
  importFmcsaDataset,
} from '../src/importers/fmcsaDataImporter';

dotenv.config({ quiet: true });

async function main() {
  const filePath = process.argv[2];

  if (!filePath) {
    throw new Error('Usage: npm run import:authority-history -- /path/to/AuthHist_All_With_History.txt');
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const startedAt = process.hrtime.bigint();
  const startedAtDate = new Date();
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const batchSize = getBatchSize();

  console.log(`starting authority history import at ${startedAtDate.toISOString()}`);
  console.log(`source file: ${filePath}`);
  console.log(`batch size: ${batchSize}`);

  try {
    const stats = await importFmcsaDataset({
      datasetType: 'authority-history',
      inputSource: filePath,
      pool,
      batchSize,
    });

    console.log(`finished authority history import at ${new Date().toISOString()}`);
    console.log(`total rows read: ${stats.rowsRead}`);
    console.log(`total rows inserted/updated: ${stats.rowsInsertedOrUpdated}`);
    console.log(`total rows failed: ${stats.rowsFailed}`);
    console.log(`total batches: ${stats.batches}`);
  } finally {
    await pool.end();
  }

  const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

  console.log(`import duration: ${(durationMs / 1000).toFixed(2)}s`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
