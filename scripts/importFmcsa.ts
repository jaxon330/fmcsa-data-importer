import dotenv from 'dotenv';
import { Pool } from 'pg';
import {
  DATASET_TYPES,
  getBatchSize,
  importFmcsaDataset,
  parseDatasetType,
  parseSourceFormat,
  previewFmcsaDataset,
  type FmcsaSourceFormat,
} from '../src/importers/fmcsaDataImporter';

dotenv.config({ quiet: true });

interface CliArgs {
  datasetType: ReturnType<typeof parseDatasetType>;
  inputSource: string;
  sourceFormat: FmcsaSourceFormat;
  dryRun: boolean;
}

function parseArgs(args: string[]): CliArgs {
  const datasetType = parseDatasetType(args[0]);
  const inputSource = args[1];
  let sourceFormat: FmcsaSourceFormat | undefined;
  let dryRun = false;

  if (!inputSource) {
    throw new Error(
      `Usage: npm run import:fmcsa -- <datasetType> <filePathOrUrl> --source <diff|allHist> [--dry-run]\nSupported datasetType values: ${DATASET_TYPES.join(', ')}`,
    );
  }

  for (let index = 2; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--source') {
      sourceFormat = parseSourceFormat(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--dry-run') {
      dryRun = true;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!sourceFormat) {
    throw new Error('--source is required and must be either "diff" or "allHist"');
  }

  return { datasetType, inputSource, sourceFormat, dryRun };
}

async function main() {
  const { datasetType, inputSource, sourceFormat, dryRun } = parseArgs(process.argv.slice(2));

  if (dryRun) {
    const preview = await previewFmcsaDataset({
      datasetType,
      inputSource,
      sourceFormat,
    });
    console.log(JSON.stringify(preview, null, 2));
    return;
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const batchSize = getBatchSize();
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  console.log(`datasetType: ${datasetType}`);
  console.log(`source: ${sourceFormat}`);
  console.log(`input source: ${inputSource}`);
  console.log(`batch size: ${batchSize}`);
  console.log(`started at: ${new Date().toISOString()}`);

  try {
    const stats = await importFmcsaDataset({
      datasetType,
      inputSource,
      sourceFormat,
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
