import dotenv from 'dotenv';
import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
  type ObjectIdentifier,
  type _Object,
} from '@aws-sdk/client-s3';
import { getFmcsaRawStorageConfig } from '../storage/fmcsaRawStorage';

dotenv.config({ quiet: true });

const DEFAULT_RETENTION_DAYS = 7;
const DELETE_BATCH_SIZE = 1_000;
const DEFAULT_LOGGED_CANDIDATES = 20;

export interface CleanupCliArgs {
  execute: boolean;
  retentionDays: number;
  prefix?: string;
  verbose: boolean;
}

export interface MotusCleanupOptions {
  bucket: string;
  prefix: string;
  retentionDays?: number;
  execute?: boolean;
  verbose?: boolean;
  now?: Date;
  s3Client?: S3Client;
}

export interface MotusCleanupSummary {
  bucket: string;
  prefix: string;
  cutoff: Date;
  scannedCount: number;
  scannedBytes: number;
  candidateCount: number;
  candidateBytes: number;
  retainedCount: number;
  protectedCount: number;
  missingLastModifiedCount: number;
  deletedCount: number;
  dryRun: boolean;
  oldestCandidate?: Date;
  newestCandidate?: Date;
}

export function parseCleanupArgs(args: string[]): CleanupCliArgs {
  let execute = false;
  let retentionDays = parseRetentionDays(process.env.MOTUS_S3_RETENTION_DAYS ?? String(DEFAULT_RETENTION_DAYS));
  let prefix: string | undefined;
  let verbose = false;

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];

    if (arg === '--execute') {
      execute = true;
      continue;
    }

    if (arg === '--retention-days') {
      retentionDays = parseRetentionDays(args[index + 1]);
      index += 1;
      continue;
    }

    if (arg === '--prefix') {
      prefix = args[index + 1];
      if (!prefix) {
        throw new Error('--prefix requires an S3 prefix');
      }
      index += 1;
      continue;
    }

    if (arg === '--verbose') {
      verbose = true;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return { execute, retentionDays, prefix, verbose };
}

export function selectExpiredMotusObjects(objects: _Object[], prefix: string, cutoff: Date): _Object[] {
  const normalizedPrefix = normalizeMotusDiffPrefix(prefix);
  const processedPrefix = `${normalizedPrefix}_processed/`;

  return objects.filter((object) => {
    if (!object.Key || !object.LastModified) {
      return false;
    }

    if (object.Key === normalizedPrefix || object.Key.startsWith(processedPrefix)) {
      return false;
    }

    return object.LastModified.getTime() < cutoff.getTime();
  });
}

export async function cleanupMotusFiles(options: MotusCleanupOptions): Promise<MotusCleanupSummary> {
  const retentionDays = options.retentionDays ?? DEFAULT_RETENTION_DAYS;
  if (!Number.isInteger(retentionDays) || retentionDays < 1) {
    throw new Error('retentionDays must be a positive integer');
  }

  const prefix = normalizeMotusDiffPrefix(options.prefix);
  const now = options.now ?? new Date();
  if (Number.isNaN(now.getTime())) {
    throw new Error('now must be a valid date');
  }

  const cutoff = new Date(now.getTime() - retentionDays * 24 * 60 * 60 * 1_000);
  const s3Client = options.s3Client ?? new S3Client({});
  const objects = await listAllObjects(s3Client, options.bucket, prefix);
  const candidates = selectExpiredMotusObjects(objects, prefix, cutoff);
  const protectedPrefix = `${prefix}_processed/`;
  const protectedCount = objects.filter((object) => object.Key?.startsWith(protectedPrefix)).length;
  const missingLastModifiedCount = objects.filter((object) => object.Key && !object.LastModified).length;
  const retainedCount = objects.length - candidates.length - protectedCount - missingLastModifiedCount;
  const candidateDates = candidates.flatMap((object) => object.LastModified ? [object.LastModified] : []);
  const summary: MotusCleanupSummary = {
    bucket: options.bucket,
    prefix,
    cutoff,
    scannedCount: objects.length,
    scannedBytes: sumObjectBytes(objects),
    candidateCount: candidates.length,
    candidateBytes: sumObjectBytes(candidates),
    retainedCount: Math.max(0, retainedCount),
    protectedCount,
    missingLastModifiedCount,
    deletedCount: 0,
    dryRun: !options.execute,
    oldestCandidate: minDate(candidateDates),
    newestCandidate: maxDate(candidateDates),
  };

  logSummary(summary);
  logCandidates(candidates, options.verbose ?? false);

  if (!options.execute || candidates.length === 0) {
    return summary;
  }

  for (let start = 0; start < candidates.length; start += DELETE_BATCH_SIZE) {
    const batch: ObjectIdentifier[] = candidates
      .slice(start, start + DELETE_BATCH_SIZE)
      .flatMap((object) => object.Key ? [{ Key: object.Key }] : []);
    const response = await s3Client.send(new DeleteObjectsCommand({
      Bucket: options.bucket,
      Delete: { Objects: batch, Quiet: false },
    }));

    if (response.Errors?.length) {
      const failures = response.Errors.map((error) => `${error.Key ?? '<unknown>'}: ${error.Code ?? 'unknown'} ${error.Message ?? ''}`);
      throw new Error(`S3 failed to delete ${response.Errors.length} object(s): ${failures.join('; ')}`);
    }

    summary.deletedCount += response.Deleted?.length ?? batch.length;
  }

  console.log(`Deleted objects: ${summary.deletedCount}`);
  return summary;
}

async function listAllObjects(s3Client: S3Client, bucket: string, prefix: string): Promise<_Object[]> {
  const objects: _Object[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    }));
    objects.push(...(response.Contents ?? []));
    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;

    if (response.IsTruncated && !continuationToken) {
      throw new Error('S3 returned a truncated listing without a continuation token');
    }
  } while (continuationToken);

  return objects;
}

function normalizeMotusDiffPrefix(prefix: string): string {
  const normalized = prefix.replace(/^\/+|\/+$/g, '');
  const segments = normalized.split('/');
  if (segments.at(-1) !== 'motusDiff') {
    throw new Error(`Refusing cleanup outside a motusDiff prefix: ${prefix}`);
  }
  return `${normalized}/`;
}

function parseRetentionDays(value: string | undefined): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error('--retention-days must be a positive integer');
  }
  return parsed;
}

function sumObjectBytes(objects: _Object[]): number {
  return objects.reduce((total, object) => total + (object.Size ?? 0), 0);
}

function minDate(dates: Date[]): Date | undefined {
  return dates.length ? new Date(Math.min(...dates.map((date) => date.getTime()))) : undefined;
}

function maxDate(dates: Date[]): Date | undefined {
  return dates.length ? new Date(Math.max(...dates.map((date) => date.getTime()))) : undefined;
}

function formatBytes(bytes: number): string {
  if (bytes < 1_024) return `${bytes} B`;
  if (bytes < 1_024 ** 2) return `${(bytes / 1_024).toFixed(1)} KiB`;
  if (bytes < 1_024 ** 3) return `${(bytes / 1_024 ** 2).toFixed(1)} MiB`;
  return `${(bytes / 1_024 ** 3).toFixed(2)} GiB`;
}

function logSummary(summary: MotusCleanupSummary): void {
  console.log('Motus S3 cleanup inventory');
  console.log(`Mode: ${summary.dryRun ? 'dry-run' : 'execute'}`);
  console.log(`Location: s3://${summary.bucket}/${summary.prefix}`);
  console.log(`Delete objects older than: ${summary.cutoff.toISOString()}`);
  console.log(`Scanned: ${summary.scannedCount} objects (${formatBytes(summary.scannedBytes)})`);
  console.log(`Delete candidates: ${summary.candidateCount} objects (${formatBytes(summary.candidateBytes)})`);
  console.log(`Retained raw objects: ${summary.retainedCount}`);
  console.log(`Protected _processed objects: ${summary.protectedCount}`);
  if (summary.missingLastModifiedCount) {
    console.log(`Skipped objects without LastModified: ${summary.missingLastModifiedCount}`);
  }
  if (summary.oldestCandidate && summary.newestCandidate) {
    console.log(`Candidate LastModified range: ${summary.oldestCandidate.toISOString()} to ${summary.newestCandidate.toISOString()}`);
  }
}

function logCandidates(candidates: _Object[], verbose: boolean): void {
  const limit = verbose ? candidates.length : Math.min(candidates.length, DEFAULT_LOGGED_CANDIDATES);
  for (const object of candidates.slice(0, limit)) {
    console.log(`candidate: ${object.LastModified?.toISOString() ?? 'unknown'} ${object.Size ?? 0} ${object.Key}`);
  }
  if (limit < candidates.length) {
    console.log(`... ${candidates.length - limit} more candidate(s); pass --verbose to list all`);
  }
}

async function run(): Promise<void> {
  const args = parseCleanupArgs(process.argv.slice(2));
  const storage = getFmcsaRawStorageConfig();
  const bucket = storage.s3BucketName;
  if (!bucket) {
    throw new Error('FMCSA_RAW_S3_BUCKET or FMCSA_S3_BUCKET_NAME is required');
  }

  await cleanupMotusFiles({
    bucket,
    prefix: args.prefix ?? `${storage.s3Prefix}/motusDiff`,
    retentionDays: args.retentionDays,
    execute: args.execute,
    verbose: args.verbose,
  });
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
