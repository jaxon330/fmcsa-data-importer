import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { Pool } from 'pg';
import {
  DATASET_TYPES,
  getBatchSize,
  importFmcsaDataset,
  parseDatasetType,
  parseNonNegativeInteger,
  parseSourceFormat,
  previewFmcsaDataset,
  type DatasetType,
  type FmcsaSourceFormat,
} from '../importers/fmcsaDataImporter';

dotenv.config({ quiet: true });

interface CliArgs {
  sourceFormat: FmcsaSourceFormat;
  datasets: DatasetType[];
  dir: string;
  dryRun: boolean;
  skipRows: number;
  progressEvery: number;
}

export interface SelectedFile {
  datasetType: DatasetType;
  filePath: string;
}

interface FilenamePattern {
  prefix: string;
  extension: string;
}

const FILENAME_PATTERNS: Record<FmcsaSourceFormat, Record<DatasetType, FilenamePattern>> = {
  diff: {
    carrier: { prefix: 'carrier_', extension: 'txt' },
    'active-insurance': { prefix: 'actpendins_', extension: 'txt' },
    'insurance-history': { prefix: 'inshist_', extension: 'txt' },
    revocation: { prefix: 'revocation_', extension: 'txt' },
    'authority-history': { prefix: 'authhist_', extension: 'txt' },
  },
  allHist: {
    carrier: { prefix: 'carrier_all_with_history_', extension: 'csv' },
    'active-insurance': { prefix: 'active_pending_insurance_all_with_history_', extension: 'csv' },
    'insurance-history': { prefix: 'insurance_history_all_with_history_', extension: 'csv' },
    revocation: { prefix: 'revocation_all_with_history_', extension: 'csv' },
    'authority-history': { prefix: 'authority_history_all_with_history_', extension: 'csv' },
  },
  motusDiff: {
    carrier: { prefix: 'motus_carrier_', extension: 'txt' },
    'active-insurance': { prefix: 'motus_actpendins_', extension: 'txt' },
    'insurance-history': { prefix: 'motus_inshist_', extension: 'txt' },
    revocation: { prefix: 'motus_revocation_', extension: 'txt' },
    'authority-history': { prefix: 'motus_authhist_', extension: 'txt' },
  },
  motusAllHist: {
    carrier: { prefix: 'motus_carrier_all_with_history_', extension: 'txt' },
    'active-insurance': { prefix: 'motus_active_pending_insurance_all_with_history_', extension: 'txt' },
    'insurance-history': { prefix: 'motus_insurance_history_all_with_history_', extension: 'txt' },
    revocation: { prefix: 'motus_revocation_all_with_history_', extension: 'txt' },
    'authority-history': { prefix: 'motus_authority_history_all_with_history_', extension: 'txt' },
  },
};

export function parseDatasetList(value: string | undefined): DatasetType[] {
  if (!value) {
    throw new Error(`--datasets is required. Supported dataset values: ${DATASET_TYPES.join(', ')}`);
  }

  const datasets = value.split(',').map((dataset) => parseDatasetType(dataset.trim()));
  if (datasets.length === 0) {
    throw new Error(`--datasets is required. Supported dataset values: ${DATASET_TYPES.join(', ')}`);
  }

  return datasets;
}

export function findLatestFmcsaFile(
  dir: string,
  sourceFormat: FmcsaSourceFormat,
  datasetType: DatasetType,
): string | null {
  const pattern = FILENAME_PATTERNS[sourceFormat][datasetType];
  const escapedPrefix = escapeRegExp(pattern.prefix);
  const escapedExtension = escapeRegExp(pattern.extension);
  const filenamePattern = new RegExp(`^${escapedPrefix}(\\d{4})_(\\d{2})_(\\d{2})\\.${escapedExtension}$`);
  const matches = fs
    .readdirSync(dir)
    .map((filename) => {
      const match = filenamePattern.exec(filename);
      if (!match) {
        return null;
      }

      return {
        filename,
        dateKey: `${match[1]}${match[2]}${match[3]}`,
      };
    })
    .filter((match): match is { filename: string; dateKey: string } => match !== null)
    .sort((left, right) => right.dateKey.localeCompare(left.dateKey));

  return matches[0] ? path.join(dir, matches[0].filename) : null;
}

export function selectBatchFiles(
  dir: string,
  sourceFormat: FmcsaSourceFormat,
  datasets: DatasetType[],
): SelectedFile[] {
  return datasets.map((datasetType) => {
    const filePath = findLatestFmcsaFile(dir, sourceFormat, datasetType);
    if (!filePath) {
      throw new Error(`No ${sourceFormat} file found for ${datasetType} in ${dir}`);
    }

    return { datasetType, filePath };
  });
}

function parseArgs(args: string[]): CliArgs {
  let sourceFormat: FmcsaSourceFormat | undefined;
  let datasets: DatasetType[] | undefined;
  let dir: string | undefined;
  let dryRun = false;
  let skipRows = 0;
  let progressEvery = 100000;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--source') {
      sourceFormat = parseSourceFormat(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--datasets') {
      datasets = parseDatasetList(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--dir') {
      dir = args[index + 1];
      if (!dir) {
        throw new Error('--dir is required');
      }
      index += 1;
      continue;
    }

    if (arg === '--dry-run') {
      dryRun = true;
      continue;
    }

    if (arg === '--skip-rows') {
      skipRows = parseNonNegativeInteger(args[index + 1], '--skip-rows');
      index += 1;
      continue;
    }

    if (arg === '--progress-every') {
      progressEvery = parseNonNegativeInteger(args[index + 1], '--progress-every');
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!sourceFormat) {
    throw new Error('--source is required and must be one of: diff, allHist, motusDiff, motusAllHist');
  }

  if (!datasets) {
    throw new Error(`--datasets is required. Supported dataset values: ${DATASET_TYPES.join(', ')}`);
  }

  if (!dir) {
    throw new Error('--dir is required');
  }

  if (skipRows > 0 && datasets.length !== 1) {
    throw new Error('--skip-rows can only be used when exactly one dataset is selected.');
  }

  return {
    sourceFormat,
    datasets,
    dir: path.resolve(process.cwd(), dir),
    dryRun,
    skipRows,
    progressEvery,
  };
}

async function runBatchImport(args: CliArgs): Promise<void> {
  const selectedFiles = selectBatchFiles(args.dir, args.sourceFormat, args.datasets);

  console.log(`source: ${args.sourceFormat}`);
  console.log(`dir: ${args.dir}`);
  console.log(`skip rows: ${args.skipRows}`);
  console.log(`progress every: ${args.progressEvery}`);
  console.log('selected files:');
  for (const selectedFile of selectedFiles) {
    console.log(`- ${selectedFile.datasetType}: ${selectedFile.filePath}`);
  }

  if (args.dryRun) {
    const previews = [];
    for (const selectedFile of selectedFiles) {
      previews.push(
        await previewFmcsaDataset({
          datasetType: selectedFile.datasetType,
          inputSource: selectedFile.filePath,
          sourceFormat: args.sourceFormat,
        }),
      );
    }

    console.log(JSON.stringify({ source: args.sourceFormat, previews }, null, 2));
    return;
  }

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const batchSize = getBatchSize();
  const summary = [];

  try {
    for (const selectedFile of selectedFiles) {
      console.log(`importing ${selectedFile.datasetType}: ${selectedFile.filePath}`);
      const stats = await importFmcsaDataset({
        datasetType: selectedFile.datasetType,
        inputSource: selectedFile.filePath,
        sourceFormat: args.sourceFormat,
        pool,
        batchSize,
        skipRows: args.skipRows,
        progressEvery: args.progressEvery,
      });
      summary.push(stats);
    }
  } finally {
    await pool.end();
  }

  console.log('');
  console.log('Summary');
  for (const stats of summary) {
    console.log(
      `${stats.datasetType}: rows read=${stats.rowsRead}, skipped=${stats.rowsSkipped}, inserted=${stats.rowsInsertedOrUpdated}, failed=${stats.rowsFailed}, batches=${stats.batches}`,
    );
  }
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

if (require.main === module) {
  runBatchImport(parseArgs(process.argv.slice(2))).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
