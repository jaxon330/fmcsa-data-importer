import dotenv from 'dotenv';
import { Pool } from 'pg';
import {
  DATASET_TYPES,
  getBatchSize,
  importFmcsaDataset,
  parseDatasetType,
} from '../src/importers/fmcsaDataImporter';

dotenv.config({ quiet: true });

async function main() {
  const datasetType = parseDatasetType(process.argv[2]);
  const inputSource = process.argv[3];

  if (!inputSource) {
    throw new Error(
      `Usage: npm run import:fmcsa -- <datasetType> <filePathOrUrl>\nSupported datasetType values: ${DATASET_TYPES.join(', ')}`,
    );
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const batchSize = getBatchSize();
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  console.log(`datasetType: ${datasetType}`);
  console.log(`input source: ${inputSource}`);
  console.log(`batch size: ${batchSize}`);
  console.log(`started at: ${new Date().toISOString()}`);

  try {
    const stats = await importFmcsaDataset({
      datasetType,
      inputSource,
      pool,
      batchSize,
    });

    console.log(`finished at: ${new Date().toISOString()}`);
    console.log(`rows read: ${stats.rowsRead}`);
    console.log(`rows inserted/updated: ${stats.rowsInsertedOrUpdated}`);
    console.log(`rows failed: ${stats.rowsFailed}`);
    console.log(`batches: ${stats.batches}`);
    console.log(`duration: ${(stats.durationMs / 1000).toFixed(2)}s`);
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
