import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import type { FmcsaDatasetKey, FmcsaDownloadMode } from '../config/fmcsaDatasets';

export type FmcsaStorageType = 'local' | 's3';

export interface FmcsaRawStorageConfig {
  storageType: FmcsaStorageType;
  localRawDataDir: string;
  s3BucketName?: string;
  s3Prefix: string;
}

export interface FmcsaRawFileRef {
  source: FmcsaDownloadMode;
  filename: string;
  localPath?: string;
  s3Key?: string;
  inputSource: string;
  displayPath: string;
}

export interface FmcsaFileValidation {
  exists: boolean;
  sizeBytes: number;
}

export interface FmcsaDownloadFileIdentity {
  etag?: string;
  lastModified?: string;
  contentLength?: number;
  sha256?: string;
}

export interface FmcsaProcessedFileIdentityRef {
  source: FmcsaDownloadMode;
  datasetKey: FmcsaDatasetKey;
  identityKey: string;
  localPath?: string;
  s3Key?: string;
  displayPath: string;
}

const DEFAULT_STORAGE_TYPE = 'local';
const DEFAULT_RAW_DATA_DIR = './data/raw';
const DEFAULT_S3_PREFIX = 'fmcsa/raw';

export function getFmcsaRawStorageConfig(dirOverride?: string): FmcsaRawStorageConfig {
  const storageType = process.env.FMCSA_STORAGE_TYPE ?? DEFAULT_STORAGE_TYPE;
  if (storageType !== 'local' && storageType !== 's3') {
    throw new Error(`Unsupported FMCSA_STORAGE_TYPE "${storageType}". Supported values: local, s3`);
  }

  const localRawDataDir = dirOverride ?? process.env.FMCSA_LOCAL_RAW_DATA_DIR ?? DEFAULT_RAW_DATA_DIR;
  const s3Prefix = trimSlashes(process.env.FMCSA_S3_PREFIX ?? DEFAULT_S3_PREFIX);
  const s3BucketName = process.env.FMCSA_S3_BUCKET_NAME;

  if (storageType === 's3' && !s3BucketName) {
    throw new Error('FMCSA_S3_BUCKET_NAME is required when FMCSA_STORAGE_TYPE=s3.');
  }

  return {
    storageType,
    localRawDataDir,
    s3BucketName,
    s3Prefix,
  };
}

export function buildFmcsaRawFileRef(
  storage: FmcsaRawStorageConfig,
  source: FmcsaDownloadMode,
  filename: string,
): FmcsaRawFileRef {
  if (storage.storageType === 's3') {
    const s3Key = [storage.s3Prefix, source, filename].filter(Boolean).join('/');
    const inputSource = `s3://${storage.s3BucketName}/${s3Key}`;
    return {
      source,
      filename,
      s3Key,
      inputSource,
      displayPath: inputSource,
    };
  }

  const localPath = path.join(getFmcsaRawDir(storage, source), filename);
  return {
    source,
    filename,
    localPath,
    inputSource: localPath,
    displayPath: toDisplayPath(localPath),
  };
}

export function buildProcessedFileIdentityRef(
  storage: FmcsaRawStorageConfig,
  source: FmcsaDownloadMode,
  datasetKey: FmcsaDatasetKey,
  identity: FmcsaDownloadFileIdentity,
): FmcsaProcessedFileIdentityRef {
  const identityKey = createProcessedFileIdentityKey(source, datasetKey, identity);
  const filename = `${identityKey}.json`;

  if (storage.storageType === 's3') {
    const s3Key = [storage.s3Prefix, source, '_processed', datasetKey, filename].filter(Boolean).join('/');
    return {
      source,
      datasetKey,
      identityKey,
      s3Key,
      displayPath: `s3://${storage.s3BucketName}/${s3Key}`,
    };
  }

  const localPath = path.join(getFmcsaRawDir(storage, source), '_processed', datasetKey, filename);
  return {
    source,
    datasetKey,
    identityKey,
    localPath,
    displayPath: toDisplayPath(localPath),
  };
}

export function getFmcsaRawDir(storage: FmcsaRawStorageConfig, source: FmcsaDownloadMode): string {
  const rawDataDir = path.resolve(process.cwd(), storage.localRawDataDir);
  return path.basename(rawDataDir) === source ? rawDataDir : path.join(rawDataDir, source);
}

export async function rawFileExists(ref: FmcsaRawFileRef, s3Client = new S3Client({})): Promise<boolean> {
  const validation = await validateRawFile(ref, s3Client);
  return validation.exists;
}

export async function processedFileIdentityExists(
  ref: FmcsaProcessedFileIdentityRef,
  storage: FmcsaRawStorageConfig,
  s3Client = new S3Client({}),
): Promise<boolean> {
  if (ref.localPath) {
    try {
      const stats = await fs.promises.stat(ref.localPath);
      return stats.isFile();
    } catch (error) {
      if (isNotFoundError(error)) {
        return false;
      }
      throw error;
    }
  }

  if (!ref.s3Key) {
    throw new Error(`Processed file identity reference is missing a local path or S3 key: ${ref.displayPath}`);
  }

  if (!storage.s3BucketName) {
    throw new Error('FMCSA_S3_BUCKET_NAME is required for S3 processed file identity checks.');
  }

  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: storage.s3BucketName, Key: ref.s3Key }));
    return true;
  } catch (error) {
    if (isNotFoundError(error)) {
      return false;
    }
    throw error;
  }
}

export async function validateRawFile(ref: FmcsaRawFileRef, s3Client = new S3Client({})): Promise<FmcsaFileValidation> {
  if (ref.localPath) {
    try {
      const stats = await fs.promises.stat(ref.localPath);
      return { exists: stats.isFile(), sizeBytes: stats.size };
    } catch (error) {
      if (isNotFoundError(error)) {
        return { exists: false, sizeBytes: 0 };
      }
      throw error;
    }
  }

  if (!ref.s3Key) {
    throw new Error(`Raw file reference is missing a local path or S3 key: ${ref.displayPath}`);
  }

  const bucket = parseBucketFromS3Url(ref.inputSource);
  try {
    const result = await s3Client.send(new HeadObjectCommand({ Bucket: bucket, Key: ref.s3Key }));
    return { exists: true, sizeBytes: result.ContentLength ?? 0 };
  } catch (error) {
    if (isNotFoundError(error)) {
      return { exists: false, sizeBytes: 0 };
    }
    throw error;
  }
}

export async function uploadRawFileToS3(
  localPath: string,
  ref: FmcsaRawFileRef,
  s3Client = new S3Client({}),
): Promise<void> {
  if (!ref.s3Key) {
    throw new Error(`S3 key is required for upload: ${ref.displayPath}`);
  }

  const bucket = parseBucketFromS3Url(ref.inputSource);
  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: ref.s3Key,
      Body: fs.createReadStream(localPath),
    }),
  );
}

export async function markProcessedFileIdentity(
  ref: FmcsaProcessedFileIdentityRef,
  storage: FmcsaRawStorageConfig,
  metadata: Record<string, unknown>,
  s3Client = new S3Client({}),
): Promise<void> {
  const body = JSON.stringify({
    ...metadata,
    source: ref.source,
    datasetKey: ref.datasetKey,
    identityKey: ref.identityKey,
    processedAt: new Date().toISOString(),
  }, null, 2);

  if (ref.localPath) {
    await fs.promises.mkdir(path.dirname(ref.localPath), { recursive: true });
    await fs.promises.writeFile(ref.localPath, body);
    return;
  }

  if (!ref.s3Key) {
    throw new Error(`Processed file identity reference is missing a local path or S3 key: ${ref.displayPath}`);
  }

  if (!storage.s3BucketName) {
    throw new Error('FMCSA_S3_BUCKET_NAME is required for S3 processed file identity markers.');
  }

  await s3Client.send(
    new PutObjectCommand({
      Bucket: storage.s3BucketName,
      Key: ref.s3Key,
      Body: body,
      ContentType: 'application/json',
    }),
  );
}

export function toDisplayPath(filePath: string): string {
  const relativePath = path.relative(process.cwd(), filePath);
  if (!relativePath || relativePath.startsWith('..')) {
    return filePath;
  }

  return relativePath;
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+|\/+$/g, '');
}

function parseBucketFromS3Url(inputSource: string): string {
  const withoutScheme = inputSource.slice('s3://'.length);
  const slashIndex = withoutScheme.indexOf('/');
  if (slashIndex <= 0) {
    throw new Error(`Invalid S3 URL: ${inputSource}`);
  }

  return withoutScheme.slice(0, slashIndex);
}

function createProcessedFileIdentityKey(
  source: FmcsaDownloadMode,
  datasetKey: FmcsaDatasetKey,
  identity: FmcsaDownloadFileIdentity,
): string {
  const stableIdentity = {
    source,
    datasetKey,
    etag: normalizeIdentityValue(identity.etag),
    lastModified: normalizeIdentityValue(identity.lastModified),
    contentLength: identity.contentLength,
    sha256: normalizeIdentityValue(identity.sha256),
  };

  return crypto.createHash('sha256').update(JSON.stringify(stableIdentity)).digest('hex');
}

function normalizeIdentityValue(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}

function isNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false;
  }

  const errorRecord = error as { name?: string; '$metadata'?: { httpStatusCode?: number }; code?: string };
  return (
    errorRecord.name === 'NotFound' ||
    errorRecord.name === 'NoSuchKey' ||
    errorRecord.code === 'ENOENT' ||
    errorRecord.$metadata?.httpStatusCode === 404
  );
}
