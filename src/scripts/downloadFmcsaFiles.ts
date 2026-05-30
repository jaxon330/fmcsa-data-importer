import { createWriteStream } from 'fs';
import { existsSync } from 'fs';
import { mkdir, rename, rm } from 'fs/promises';
import dotenv from 'dotenv';
import path from 'path';
import process from 'process';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';
import {
  buildFmcsaDownloadUrl,
  buildFmcsaSodaExportUrl,
  FMCSA_DATASETS,
  type FmcsaDatasetKey,
  type FmcsaDownloadMode,
} from '../config/fmcsaDatasets';

interface CliArgs {
  downloadMode: FmcsaDownloadMode;
  force: boolean;
  datasetKeys?: FmcsaDatasetKey[];
}

interface DownloadResult {
  datasetKey: FmcsaDatasetKey;
  savedPath?: string;
  skipped: boolean;
  failed: boolean;
}

const DOWNLOAD_MODES = ['diff', 'allHist'] as const;
const DATASET_KEY_ALIASES: Record<string, FmcsaDatasetKey> = {
  carrier: 'carrier',
  activeInsurance: 'activeInsurance',
  'active-insurance': 'activeInsurance',
  insuranceHistory: 'insuranceHistory',
  'insurance-history': 'insuranceHistory',
  revocation: 'revocation',
  authorityHistory: 'authorityHistory',
  'authority-history': 'authorityHistory',
};
const DEFAULT_STORAGE_TYPE = 'local';
const DEFAULT_RAW_DATA_DIR = './data/raw';

dotenv.config({ quiet: true });

function parseArgs(args: string[]): CliArgs {
  let downloadMode: FmcsaDownloadMode | undefined;
  let force = false;
  let datasetKeys: FmcsaDatasetKey[] | undefined;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--download') {
      const value = args[index + 1];
      if (!isDownloadMode(value)) {
        throw new Error('--download is required and must be either "diff" or "allHist"');
      }
      downloadMode = value;
      index += 1;
      continue;
    }

    if (arg === '--force') {
      force = true;
      continue;
    }

    if (arg === '--datasets') {
      datasetKeys = parseDatasetKeys(args[index + 1]);
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!downloadMode) {
    throw new Error('--download is required and must be either "diff" or "allHist"');
  }

  return { downloadMode, force, datasetKeys };
}

function isDownloadMode(value: string | undefined): value is FmcsaDownloadMode {
  return DOWNLOAD_MODES.some((mode) => mode === value);
}

function parseDatasetKeys(value: string | undefined): FmcsaDatasetKey[] {
  if (!value) {
    throw new Error('--datasets requires a comma-separated dataset list');
  }

  return value.split(',').map((rawDatasetKey) => {
    const datasetKey = DATASET_KEY_ALIASES[rawDatasetKey.trim()];
    if (!datasetKey) {
      throw new Error(`Unsupported dataset "${rawDatasetKey}". Supported values: ${Object.keys(DATASET_KEY_ALIASES).join(', ')}`);
    }

    return datasetKey;
  });
}

function formatDateForFilename(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');

  return `${year}_${month}_${day}`;
}

function buildFilename(filePrefix: string, extension: string, date: Date): string {
  return `${filePrefix}_${formatDateForFilename(date)}.${extension}`;
}

function toDisplayPath(filePath: string): string {
  const relativePath = path.relative(process.cwd(), filePath);
  if (!relativePath || relativePath.startsWith('..')) {
    return filePath;
  }

  return relativePath;
}

async function downloadToFile(url: string, targetPath: string): Promise<void> {
  const tempPath = `${targetPath}.tmp`;

  await rm(tempPath, { force: true });

  try {
    const response = await fetch(url);
    if (!response.ok || !response.body) {
      throw new Error(`Download failed: ${response.status} ${response.statusText} (${url})`);
    }

    const fileStream = createWriteStream(tempPath);
    await pipeline(Readable.fromWeb(response.body as any), fileStream);
    await rename(tempPath, targetPath);
  } catch (error) {
    await rm(tempPath, { force: true });
    throw error;
  }
}

async function run(): Promise<void> {
  const { downloadMode, force, datasetKeys } = parseArgs(process.argv.slice(2));
  const storageType = process.env.FMCSA_STORAGE_TYPE ?? DEFAULT_STORAGE_TYPE;
  const rawDataDir = process.env.FMCSA_LOCAL_RAW_DATA_DIR ?? DEFAULT_RAW_DATA_DIR;

  if (storageType !== 'local') {
    throw new Error(`Unsupported FMCSA_STORAGE_TYPE "${storageType}". Only "local" is implemented.`);
  }

  const outputFolder = path.resolve(process.cwd(), rawDataDir, downloadMode);
  const socrataAppToken = process.env.FMCSA_SOCRATA_APP_TOKEN ?? process.env.SOCRATA_APP_TOKEN;
  await mkdir(outputFolder, { recursive: true });

  const results: DownloadResult[] = [];
  const today = new Date();
  const datasets = FMCSA_DATASETS[downloadMode];
  const selectedDatasetKeys = datasetKeys ?? (Object.keys(datasets) as FmcsaDatasetKey[]);

  for (const datasetKey of selectedDatasetKeys) {
    const dataset = datasets[datasetKey];
    const filename = buildFilename(dataset.filePrefix, dataset.extension, today);
    const targetPath = path.join(outputFolder, filename);

    console.log(`Downloading ${datasetKey}...`);

    if (existsSync(targetPath) && !force) {
      console.log('Skipped: already exists');
      results.push({ datasetKey, savedPath: targetPath, skipped: true, failed: false });
      continue;
    }

    try {
      const downloadUrl =
        downloadMode === 'diff'
          ? buildFmcsaDownloadUrl(dataset.datasetId)
          : buildFmcsaSodaExportUrl(dataset.datasetId, socrataAppToken);

      await downloadToFile(downloadUrl, targetPath);
      console.log(`Saved: ${toDisplayPath(targetPath)}`);
      results.push({ datasetKey, savedPath: targetPath, skipped: false, failed: false });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Failed: ${message}`);
      results.push({ datasetKey, skipped: false, failed: true });
    }
  }

  const downloadedCount = results.filter((result) => !result.skipped && !result.failed).length;
  const skippedCount = results.filter((result) => result.skipped).length;
  const failedCount = results.filter((result) => result.failed).length;
  const savedFiles = results
    .filter((result) => result.savedPath && !result.skipped && !result.failed)
    .map((result) => toDisplayPath(result.savedPath as string));

  console.log('');
  console.log('Summary');
  console.log(`Mode: ${downloadMode}`);
  console.log(`Output folder: ${toDisplayPath(outputFolder)}`);
  console.log(`Downloaded: ${downloadedCount}`);
  console.log(`Skipped: ${skippedCount}`);
  console.log(`Failed: ${failedCount}`);
  console.log('Saved files:');
  if (savedFiles.length === 0) {
    console.log('- none');
  } else {
    for (const savedFile of savedFiles) {
      console.log(`- ${savedFile}`);
    }
  }

  if (failedCount > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});
