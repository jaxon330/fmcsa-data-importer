import { DeleteObjectsCommand, ListObjectsV2Command, type S3Client } from '@aws-sdk/client-s3';
import {
  cleanupMotusFiles,
  parseCleanupArgs,
  selectExpiredMotusObjects,
} from '../src/scripts/cleanupMotusFiles';

describe('Motus S3 cleanup', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('defaults to a safe seven-day dry run', () => {
    expect(parseCleanupArgs([])).toEqual({
      execute: false,
      retentionDays: 7,
      prefix: undefined,
      verbose: false,
    });
  });

  it('selects expired raw files but protects recent files and processed markers', () => {
    const cutoff = new Date('2026-10-03T12:00:00.000Z');
    const selected = selectExpiredMotusObjects([
      { Key: 'fmcsa/dataset/motusDiff/old.csv', LastModified: new Date('2026-10-03T11:59:59.999Z'), Size: 10 },
      { Key: 'fmcsa/dataset/motusDiff/recent.csv', LastModified: cutoff, Size: 20 },
      { Key: 'fmcsa/dataset/motusDiff/_processed/carrier/old.json', LastModified: new Date('2026-09-01T00:00:00Z'), Size: 30 },
      { Key: 'fmcsa/dataset/motusDiff/no-date.csv', Size: 40 },
    ], 'fmcsa/dataset/motusDiff', cutoff);

    expect(selected.map((object) => object.Key)).toEqual(['fmcsa/dataset/motusDiff/old.csv']);
  });

  it('refuses to operate on a prefix other than motusDiff', () => {
    expect(() => selectExpiredMotusObjects([], 'fmcsa/dataset', new Date())).toThrow(
      'Refusing cleanup outside a motusDiff prefix',
    );
  });

  it('paginates inventory and does not delete in dry-run mode', async () => {
    const send = jest.fn()
      .mockResolvedValueOnce({
        Contents: [{ Key: 'fmcsa/dataset/motusDiff/old-1.csv', LastModified: new Date('2026-09-01T00:00:00Z'), Size: 10 }],
        IsTruncated: true,
        NextContinuationToken: 'page-2',
      })
      .mockResolvedValueOnce({
        Contents: [{ Key: 'fmcsa/dataset/motusDiff/recent.csv', LastModified: new Date('2026-10-09T00:00:00Z'), Size: 20 }],
        IsTruncated: false,
      });
    const s3Client = { send } as unknown as S3Client;
    jest.spyOn(console, 'log').mockImplementation(() => undefined);

    const summary = await cleanupMotusFiles({
      bucket: 'bucket',
      prefix: 'fmcsa/dataset/motusDiff',
      now: new Date('2026-10-10T00:00:00Z'),
      s3Client,
    });

    expect(summary).toMatchObject({ scannedCount: 2, candidateCount: 1, deletedCount: 0, dryRun: true });
    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls.every(([command]) => command instanceof ListObjectsV2Command)).toBe(true);
  });

  it('deletes expired files only when execute is enabled', async () => {
    const send = jest.fn(async (command: unknown) => {
      if (command instanceof ListObjectsV2Command) {
        return {
          Contents: [
            { Key: 'fmcsa/dataset/motusDiff/old.csv', LastModified: new Date('2026-09-01T00:00:00Z'), Size: 10 },
            { Key: 'fmcsa/dataset/motusDiff/_processed/carrier/old.json', LastModified: new Date('2026-09-01T00:00:00Z'), Size: 10 },
          ],
          IsTruncated: false,
        };
      }
      if (command instanceof DeleteObjectsCommand) {
        return { Deleted: [{ Key: 'fmcsa/dataset/motusDiff/old.csv' }] };
      }
      throw new Error('Unexpected command');
    });
    const s3Client = { send } as unknown as S3Client;
    jest.spyOn(console, 'log').mockImplementation(() => undefined);

    const summary = await cleanupMotusFiles({
      bucket: 'bucket',
      prefix: 'fmcsa/dataset/motusDiff',
      retentionDays: 7,
      execute: true,
      now: new Date('2026-10-10T00:00:00Z'),
      s3Client,
    });

    expect(summary.deletedCount).toBe(1);
    const deleteCommand = send.mock.calls.map(([command]) => command).find((command) => command instanceof DeleteObjectsCommand) as DeleteObjectsCommand;
    expect(deleteCommand.input.Delete?.Objects).toEqual([{ Key: 'fmcsa/dataset/motusDiff/old.csv' }]);
  });
});
