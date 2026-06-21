import { createWriteStream } from 'fs';
import crypto from 'crypto';
import os from 'os';
import { mkdir, rename, rm } from 'fs/promises';
import dotenv from 'dotenv';
import path from 'path';
import process from 'process';
import { Readable, Transform } from 'stream';
import { pipeline } from 'stream/promises';
import { S3Client } from '@aws-sdk/client-s3';
import {
  buildFmcsaDownloadUrl,
  buildFmcsaSodaExportUrl,
  buildMotusRowsCsvDownloadUrl,
  datasetKeyToName,
  FMCSA_DATASETS,
  MOTUS_DATASETS,
  toRawSource,
  type FmcsaDatasetKey,
  type FmcsaDownloadMode,
  type FmcsaProvider,
  type FmcsaRawSource,
  parseFmcsaDatasetKeys,
} from '../config/fmcsaDatasets';
import {
  buildProcessedFileIdentityRef,
  buildFmcsaRawFileRef,
  getFmcsaRawDir,
  getFmcsaRawStorageConfig,
  processedFileIdentityExists,
  rawFileExists,
  toDisplayPath,
  uploadRawFileToS3,
  type FmcsaDownloadFileIdentity,
  type FmcsaRawFileRef,
} from '../storage/fmcsaRawStorage';

interface CliArgs {
  downloadMode: FmcsaDownloadMode;
  provider: FmcsaProvider;
  force: boolean;
  datasetKeys?: FmcsaDatasetKey[];
  dir?: string;
}

export interface DownloadResult {
  datasetKey: FmcsaDatasetKey;
  datasetName: string;
  fileRef?: FmcsaRawFileRef;
  downloadIdentity?: FmcsaDownloadFileIdentity;
  skippedReason?: 'already_exists' | 'already_processed' | 'not_published';
  skipped: boolean;
  failed: boolean;
  error?: string;
}

const DOWNLOAD_MODES = ['diff', 'allHist'] as const;
const PROVIDERS = ['legacy', 'motus'] as const;
const DEFAULT_DOWNLOAD_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_DOWNLOAD_ATTEMPTS = 3;
const DEFAULT_RETRY_BASE_DELAY_MS = 1_000;
const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504]);
const RETRYABLE_ERROR_CODES = new Set(['ETIMEDOUT', 'ECONNRESET']);

dotenv.config({ quiet: true });

interface DownloadToFileOptions {
  headers?: HeadersInit;
  datasetName?: string;
  source?: FmcsaRawSource;
  maxAttempts?: number;
  timeoutMs?: number;
  retryBaseDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export interface DownloadToFileResult {
  identity: FmcsaDownloadFileIdentity;
}

export class DownloadHttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly statusText: string,
  ) {
    super(`Download failed: ${status} ${statusText}`);
  }
}

function parseArgs(args: string[]): CliArgs {
  let downloadMode: FmcsaDownloadMode | undefined;
  let provider: FmcsaProvider = 'legacy';
  let force = false;
  let datasetKeys: FmcsaDatasetKey[] | undefined;
  let dir: string | undefined;

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

    if (arg === '--provider') {
      const value = args[index + 1];
      if (!isProvider(value)) {
        throw new Error('--provider must be either "legacy" or "motus"');
      }
      provider = value;
      index += 1;
      continue;
    }

    if (arg === '--datasets') {
      datasetKeys = parseFmcsaDatasetKeys(args[index + 1]);
      index += 1;
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

  if (!downloadMode) {
    throw new Error('--download is required and must be either "diff" or "allHist"');
  }

  return { downloadMode, provider, force, datasetKeys, dir };
}

function isDownloadMode(value: string | undefined): value is FmcsaDownloadMode {
  return DOWNLOAD_MODES.some((mode) => mode === value);
}

function isProvider(value: string | undefined): value is FmcsaProvider {
  return PROVIDERS.some((provider) => provider === value);
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

export async function downloadToFile(url: string, targetPath: string, options: DownloadToFileOptions = {}): Promise<DownloadToFileResult> {
  const tempPath = `${targetPath}.tmp`;
  const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_DOWNLOAD_ATTEMPTS;
  const timeoutMs = options.timeoutMs ?? DEFAULT_DOWNLOAD_TIMEOUT_MS;
  const retryBaseDelayMs = options.retryBaseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS;
  const sleep = options.sleep ?? delay;
  const random = options.random ?? Math.random;

  await rm(tempPath, { force: true });

  try {
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const response = await fetchWithTimeout(url, options.headers, timeoutMs);
        if (!response.ok || !response.body) {
          if (response.status === 403 && url.includes('/export.csv')) {
            throw new Error('Socrata app token required. Set FMCSA_SOCRATA_APP_TOKEN.');
          }

          const error = new DownloadHttpError(response.status, response.statusText);
          if (response.ok || !isRetryableStatus(response.status) || attempt === maxAttempts) {
            throw error;
          }

          logRetryAttempt({ url, attempt, maxAttempts, status: response.status, datasetName: options.datasetName, source: options.source });
          await sleep(computeRetryDelayMs(attempt, retryBaseDelayMs, random));
          continue;
        }

        const identity = getResponseIdentity(response);
        const hash = crypto.createHash('sha256');
        const fileStream = createWriteStream(tempPath);
        const hashStream = new Transform({
          transform(chunk, _encoding, callback) {
            hash.update(chunk);
            callback(null, chunk);
          },
        });
        await pipeline(Readable.fromWeb(response.body as any), hashStream, fileStream);
        identity.sha256 = hash.digest('hex');
        await rename(tempPath, targetPath);
        return { identity };
      } catch (error) {
        if (!isRetryableNetworkError(error) || attempt === maxAttempts) {
          throw error;
        }

        logRetryAttempt({
          url,
          attempt,
          maxAttempts,
          error,
          datasetName: options.datasetName,
          source: options.source,
        });
        await sleep(computeRetryDelayMs(attempt, retryBaseDelayMs, random));
      }
    }

    throw new Error('Download failed after retry attempts');
  } catch (error) {
    await rm(tempPath, { force: true });
    throw error;
  }
}

async function fetchWithTimeout(url: string, headers: HeadersInit | undefined, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, { headers, signal: controller.signal });
  } catch (error) {
    if (isAbortError(error)) {
      const timeoutError = new Error(`Download timed out after ${timeoutMs}ms`);
      (timeoutError as NodeJS.ErrnoException).code = 'ETIMEDOUT';
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function isRetryableStatus(status: number): boolean {
  return RETRYABLE_HTTP_STATUSES.has(status);
}

function isRetryableNetworkError(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false;
  }

  const errorRecord = error as { code?: string; cause?: { code?: string } };
  return RETRYABLE_ERROR_CODES.has(errorRecord.code ?? '') || RETRYABLE_ERROR_CODES.has(errorRecord.cause?.code ?? '');
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError';
}

function computeRetryDelayMs(attempt: number, baseDelayMs: number, random: () => number): number {
  const exponentialDelay = baseDelayMs * 2 ** (attempt - 1);
  const jitter = Math.floor(random() * baseDelayMs);
  return exponentialDelay + jitter;
}

function logRetryAttempt(input: {
  url: string;
  attempt: number;
  maxAttempts: number;
  status?: number;
  error?: unknown;
  datasetName?: string;
  source?: FmcsaRawSource;
}): void {
  const retryReason = input.status ? `HTTP ${input.status}` : formatRetryableError(input.error);
  console.warn(
    [
      'Transient FMCSA download failure.',
      `attempt=${input.attempt}/${input.maxAttempts}`,
      `dataset=${input.datasetName ?? 'unknown'}`,
      `source=${input.source ?? 'unknown'}`,
      `url=${sanitizeUrl(input.url)}`,
      `reason=${retryReason}`,
    ].join(' '),
  );
}

function sanitizeUrl(url: string): string {
  try {
    const parsed = new URL(url);
    return `${parsed.host}${parsed.pathname}`;
  } catch {
    return 'invalid-url';
  }
}

function formatRetryableError(error: unknown): string {
  if (error instanceof Error) {
    const errorRecord = error as NodeJS.ErrnoException & { cause?: { code?: string } };
    return errorRecord.code ?? errorRecord.cause?.code ?? error.name;
  }
  return 'network-error';
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function downloadFmcsaFiles(options: {
  downloadMode: FmcsaDownloadMode;
  provider?: FmcsaProvider;
  force?: boolean;
  datasetKeys?: FmcsaDatasetKey[];
  dir?: string;
  date?: Date;
}): Promise<DownloadResult[]> {
  const provider = options.provider ?? 'legacy';
  const rawSource = toRawSource(provider, options.downloadMode);
  const storage = getFmcsaRawStorageConfig(options.dir);
  const socrataAppToken = process.env.FMCSA_SOCRATA_APP_TOKEN ?? process.env.SOCRATA_APP_TOKEN;
  const s3Client = new S3Client({});
  const localDownloadFolder =
    storage.storageType === 'local'
      ? getFmcsaRawDir(storage, rawSource)
      : path.join(os.tmpdir(), 'fmcsa-data-importer', rawSource);
  await mkdir(localDownloadFolder, { recursive: true });

  const results: DownloadResult[] = [];
  const today = options.date ?? new Date();
  const datasets = provider === 'motus' ? MOTUS_DATASETS[options.downloadMode] : FMCSA_DATASETS[options.downloadMode];
  const selectedDatasetKeys = options.datasetKeys ?? (Object.keys(datasets) as FmcsaDatasetKey[]);

  for (const datasetKey of selectedDatasetKeys) {
    const dataset = getDatasetDownloadConfig(provider, options.downloadMode, datasetKey);
    if (!dataset) {
      results.push({
        datasetKey,
        datasetName: datasetKeyToName(datasetKey),
        skipped: false,
        failed: true,
        error: `${provider} ${options.downloadMode} does not support dataset ${datasetKeyToName(datasetKey)}`,
      });
      continue;
    }

    const filename = buildFilename(dataset.filePrefix, dataset.extension, today);
    const fileRef = buildFmcsaRawFileRef(storage, rawSource, filename);
    const targetPath = fileRef.localPath ?? path.join(localDownloadFolder, filename);
    const datasetName = datasetKeyToName(datasetKey);

    console.log(`Downloading ${datasetName}...`);

    if (!options.force && await rawFileExists(fileRef, s3Client)) {
      console.log('Skipped: already exists');
      results.push({ datasetKey, datasetName, fileRef, skippedReason: 'already_exists', skipped: true, failed: false });
      continue;
    }

    try {
      const downloadUrl =
        provider === 'motus'
          ? buildMotusRowsCsvDownloadUrl(dataset.datasetId)
          : options.downloadMode === 'diff'
          ? buildFmcsaDownloadUrl(dataset.datasetId)
          : buildFmcsaSodaExportUrl(dataset.datasetId);
      const headers = provider === 'legacy' && socrataAppToken ? { 'X-App-Token': socrataAppToken } : undefined;

      const download = await downloadToFile(downloadUrl, targetPath, {
        headers,
        datasetName,
        source: rawSource,
      });
      if ((provider === 'motus' || options.downloadMode === 'diff') && !options.force) {
        const processedRef = buildProcessedFileIdentityRef(storage, rawSource, datasetKey, download.identity);
        if (await processedFileIdentityExists(processedRef, storage, s3Client)) {
          await rm(targetPath, { force: true });
          console.log(`Skipped: already processed file identity (${processedRef.identityKey})`);
          results.push({
            datasetKey,
            datasetName,
            fileRef,
            downloadIdentity: download.identity,
            skippedReason: 'already_processed',
            skipped: true,
            failed: false,
          });
          continue;
        }
      }
      if (fileRef.s3Key) {
        await uploadRawFileToS3(targetPath, fileRef, s3Client);
        await rm(targetPath, { force: true });
      }
      console.log(`Saved: ${fileRef.displayPath}`);
      results.push({ datasetKey, datasetName, fileRef, downloadIdentity: download.identity, skipped: false, failed: false });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (options.downloadMode === 'diff' && error instanceof DownloadHttpError && error.status === 404) {
        console.log('Skipped: daily diff file not published yet (HTTP 404)');
        results.push({
          datasetKey,
          datasetName,
          fileRef,
          skippedReason: 'not_published',
          skipped: true,
          failed: false,
          error: message,
        });
        continue;
      }

      console.error(`Failed: ${message}`);
      results.push({ datasetKey, datasetName, fileRef, skipped: false, failed: true, error: message });
    }
  }

  return results;
}

function getDatasetDownloadConfig(
  provider: FmcsaProvider,
  downloadMode: FmcsaDownloadMode,
  datasetKey: FmcsaDatasetKey,
) {
  const datasets = provider === 'motus' ? MOTUS_DATASETS[downloadMode] : FMCSA_DATASETS[downloadMode];
  if (!(datasetKey in datasets)) {
    return undefined;
  }

  return datasets[datasetKey as keyof typeof datasets];
}

function getResponseIdentity(response: Response): FmcsaDownloadFileIdentity {
  const contentLength = response.headers.get('content-length');
  const parsedContentLength = contentLength === null ? undefined : Number(contentLength);

  return {
    etag: response.headers.get('etag') ?? undefined,
    lastModified: response.headers.get('last-modified') ?? undefined,
    contentLength: Number.isFinite(parsedContentLength) ? parsedContentLength : undefined,
  };
}

async function run(): Promise<void> {
  const { downloadMode, provider, force, datasetKeys, dir } = parseArgs(process.argv.slice(2));
  const storage = getFmcsaRawStorageConfig(dir);
  const rawSource = toRawSource(provider, downloadMode);
  const outputFolder =
    storage.storageType === 'local'
      ? toDisplayPath(getFmcsaRawDir(storage, rawSource))
      : `s3://${storage.s3BucketName}/${[storage.s3Prefix, rawSource].filter(Boolean).join('/')}`;
  const results = await downloadFmcsaFiles({ downloadMode, provider, force, datasetKeys, dir });
  const downloadedCount = results.filter((result) => !result.skipped && !result.failed).length;
  const skippedCount = results.filter((result) => result.skipped).length;
  const failedCount = results.filter((result) => result.failed).length;
  const savedFiles = results
    .filter((result) => result.fileRef && !result.skipped && !result.failed)
    .map((result) => result.fileRef?.displayPath as string);

  console.log('');
  console.log('Summary');
  console.log(`Mode: ${downloadMode}`);
  console.log(`Provider: ${provider}`);
  console.log(`Output folder: ${outputFolder}`);
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

if (require.main === module) {
  run().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(message);
    process.exitCode = 1;
  });
}
