import dotenv from 'dotenv';
import { S3Client } from '@aws-sdk/client-s3';
import { Pool } from 'pg';
import {
  BROKER_CHECK_V1_DATASETS,
  datasetKeyToName,
  FMCSA_DATASETS,
  parseFmcsaDatasetKeys,
  type FmcsaDatasetKey,
  type FmcsaDownloadMode,
} from '../config/fmcsaDatasets';
import {
  getBatchSize,
  importFmcsaDataset,
  previewFmcsaDataset,
  parseSourceFormat,
  type DatasetType,
  type ImportStats,
} from '../importers/fmcsaDataImporter';
import { downloadFmcsaFiles, type DownloadResult } from './downloadFmcsaFiles';
import {
  buildProcessedFileIdentityRef,
  buildFmcsaRawFileRef,
  getFmcsaRawStorageConfig,
  markProcessedFileIdentity,
  validateRawFile,
  type FmcsaDownloadFileIdentity,
  type FmcsaFileValidation,
  type FmcsaRawFileRef,
} from '../storage/fmcsaRawStorage';

dotenv.config({ quiet: true });

interface CliArgs {
  source: FmcsaDownloadMode;
  datasets: FmcsaDatasetKey[];
  dryRun: boolean;
  force: boolean;
  dir?: string;
}

interface DatasetSyncSummary {
  datasetKey: FmcsaDatasetKey;
  datasetType: DatasetType;
  fileRef?: FmcsaRawFileRef;
  download?: DownloadResult;
  downloadIdentity?: FmcsaDownloadFileIdentity;
  validation?: FmcsaFileValidation;
  importStats?: ImportStats;
  dryRunRowsPreviewed?: number;
  skipped: boolean;
  skippedReason?: DownloadResult['skippedReason'];
  failed: boolean;
  failedStep?: 'download' | 'validate' | 'import';
  error?: string;
}

function parseArgs(args: string[]): CliArgs {
  let source: FmcsaDownloadMode | undefined;
  let datasets: FmcsaDatasetKey[] | undefined;
  let dryRun = false;
  let force = false;
  let dir: string | undefined;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--source') {
      source = parseSourceFormat(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--datasets') {
      datasets = parseFmcsaDatasetKeys(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--dry-run') {
      dryRun = true;
      continue;
    }

    if (arg === '--force') {
      force = true;
      continue;
    }

    if (arg === '--dir') {
      dir = args[index + 1];
      if (!dir) {
        throw new Error('--dir requires a directory path');
      }
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!source) {
    throw new Error('--source is required and must be either "diff" or "allHist"');
  }

  return {
    source,
    datasets: datasets ?? parseFmcsaDatasetKeys(undefined, BROKER_CHECK_V1_DATASETS),
    dryRun,
    force,
    dir,
  };
}

async function run(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const storage = getFmcsaRawStorageConfig(args.dir);
  const s3Client = new S3Client({});
  const batchSize = getBatchSize();
  const summaries = new Map<FmcsaDatasetKey, DatasetSyncSummary>();

  for (const datasetKey of args.datasets) {
    summaries.set(datasetKey, {
      datasetKey,
      datasetType: datasetKeyToName(datasetKey) as DatasetType,
      skipped: false,
      failed: false,
    });
  }

  console.log('FMCSA sync started');
  console.log(`source: ${args.source}`);
  console.log(`datasets: ${args.datasets.map(datasetKeyToName).join(',')}`);
  console.log(`dry-run: ${args.dryRun ? 'yes' : 'no'}`);
  console.log(`force: ${args.force ? 'yes' : 'no'}`);
  console.log(`storage: ${storage.storageType}`);
  console.log('');

  console.log('Downloading files...');
  const downloadResults = await downloadFmcsaFiles({
    downloadMode: args.source,
    datasetKeys: args.datasets,
    force: args.force,
    dir: args.dir,
  });

  for (const result of downloadResults) {
    const summary = summaries.get(result.datasetKey);
    if (!summary) {
      continue;
    }

    summary.download = result;
    summary.fileRef = result.fileRef;
    summary.downloadIdentity = result.downloadIdentity;
    if (result.skipped) {
      summary.skipped = true;
      summary.skippedReason = result.skippedReason;
      summary.error = result.error;
    }
    if (result.failed) {
      summary.failed = true;
      summary.failedStep = 'download';
      summary.error = result.error ?? 'download failed';
    }
  }

  const downloadedCount = downloadResults.filter((result) => !result.failed && !result.skipped).length;
  const skippedCount = downloadResults.filter((result) => result.skipped).length;
  const failedDownloadCount = downloadResults.filter((result) => result.failed).length;
  console.log(`Downloaded/skipped/failed summary: ${downloadedCount}/${skippedCount}/${failedDownloadCount}`);
  console.log('');

  console.log('Validating files...');
  for (const datasetKey of args.datasets) {
    const summary = summaries.get(datasetKey);
    if (!summary || summary.failed || summary.skipped) {
      continue;
    }

    try {
      const dataset = FMCSA_DATASETS[args.source][datasetKey];
      const fileRef = summary.fileRef ?? buildFmcsaRawFileRef(
        storage,
        args.source,
        buildFilename(dataset.filePrefix, dataset.extension, new Date()),
      );
      const validation = await validateRawFile(fileRef, s3Client);
      summary.fileRef = fileRef;
      summary.validation = validation;

      console.log(`${datasetKeyToName(datasetKey)}: file exists=${validation.exists} size=${validation.sizeBytes}`);
      if (!validation.exists || validation.sizeBytes <= 0) {
        throw new Error(`File missing or empty: ${fileRef.displayPath}`);
      }
    } catch (error) {
      summary.failed = true;
      summary.failedStep = 'validate';
      summary.error = error instanceof Error ? error.message : String(error);
      console.error(`Validation failed for ${datasetKeyToName(datasetKey)}: ${summary.error}`);
    }
  }
  console.log('');

  console.log('Importing files...');
  let pool: Pool | undefined;
  if (!args.dryRun) {
    if (!process.env.DATABASE_URL) {
      throw new Error('DATABASE_URL is required.');
    }
    pool = new Pool({ connectionString: process.env.DATABASE_URL });
  }

  try {
    for (const datasetKey of args.datasets) {
      const summary = summaries.get(datasetKey);
      if (!summary || summary.failed || summary.skipped) {
        continue;
      }

      if (!summary.fileRef) {
        summary.failed = true;
        summary.failedStep = 'import';
        summary.error = 'No file selected for import.';
        continue;
      }

      try {
        console.log(`Importing ${summary.datasetType}...`);

        if (args.dryRun) {
          const preview = await previewFmcsaDataset({
            datasetType: summary.datasetType,
            inputSource: summary.fileRef.inputSource,
            sourceFormat: args.source,
            s3Client,
          });
          summary.dryRunRowsPreviewed = preview.preview.length;
          console.log(`rows read: preview only`);
          console.log(`rows inserted/updated: 0`);
          console.log(`rows failed: 0`);
          console.log(`preview rows: ${preview.preview.length}`);
          continue;
        }

        const stats = await importFmcsaDataset({
          datasetType: summary.datasetType,
          inputSource: summary.fileRef.inputSource,
          sourceFormat: args.source,
          pool: pool as Pool,
          batchSize,
          progressEvery: 100000,
          s3Client,
        });
        summary.importStats = stats;
        console.log(`rows read: ${stats.rowsRead}`);
        console.log(`rows inserted/updated: ${stats.rowsInsertedOrUpdated}`);
        console.log(`rows failed: ${stats.rowsFailed}`);

        if (summary.downloadIdentity) {
          const processedRef = buildProcessedFileIdentityRef(
            storage,
            args.source,
            datasetKey,
            summary.downloadIdentity,
          );
          await markProcessedFileIdentity(processedRef, storage, {
            datasetType: summary.datasetType,
            file: summary.fileRef.displayPath,
            identity: summary.downloadIdentity,
            importStats: stats,
          }, s3Client);
          console.log(`processed identity recorded: ${processedRef.identityKey}`);
        }
      } catch (error) {
        summary.failed = true;
        summary.failedStep = 'import';
        summary.error = error instanceof Error ? error.message : String(error);
        console.error(`Import failed for ${summary.datasetType}: ${summary.error}`);
      }
    }
  } finally {
    await pool?.end();
  }

  console.log('');
  printFinalSummary([...summaries.values()]);

  const failed = [...summaries.values()].filter((summary) => summary.failed);
  if (failed.length > 0) {
    console.error('');
    console.error('FMCSA sync failed');
    for (const failure of failed) {
      console.error(`dataset: ${failure.datasetType}`);
      console.error(`step: ${failure.failedStep ?? 'unknown'}`);
      console.error(`error: ${failure.error ?? 'unknown error'}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log('FMCSA sync completed');
}

function printFinalSummary(summaries: DatasetSyncSummary[]): void {
  console.log('Summary');
  for (const summary of summaries) {
    const validation = summary.validation;
    const stats = summary.importStats;
    const status = summary.failed
      ? `failed at ${summary.failedStep}`
      : summary.skipped
        ? `skipped (${formatSkippedReason(summary.skippedReason)})`
        : 'succeeded';
    console.log(
      [
        `${summary.datasetType}: ${status}`,
        `file=${summary.fileRef?.displayPath ?? 'none'}`,
        `size=${validation?.sizeBytes ?? 0}`,
        `rows read=${stats?.rowsRead ?? (summary.dryRunRowsPreviewed === undefined ? 0 : 'preview only')}`,
        `inserted/updated=${stats?.rowsInsertedOrUpdated ?? 0}`,
        `rows failed=${stats?.rowsFailed ?? 0}`,
      ].join(', '),
    );
  }
}

function formatSkippedReason(reason: DownloadResult['skippedReason']): string {
  if (reason === 'not_published') {
    return 'daily diff not published yet';
  }
  if (reason === 'already_processed') {
    return 'already processed file identity';
  }
  if (reason === 'already_exists') {
    return 'raw file already exists';
  }
  return 'not downloaded';
}

function buildFilename(filePrefix: string, extension: string, date: Date): string {
  return `${filePrefix}_${formatDateForFilename(date)}.${extension}`;
}

function formatDateForFilename(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');

  return `${year}_${month}_${day}`;
}

run().catch((error) => {
  console.error('FMCSA sync failed');
  console.error(`dataset: unknown`);
  console.error(`step: unknown`);
  console.error(`error: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
