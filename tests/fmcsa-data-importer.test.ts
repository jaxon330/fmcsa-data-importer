import fs from 'fs';
import os from 'os';
import path from 'path';
import { Readable } from 'stream';
import {
  buildUpsertBatch,
  createSourceRecordHash,
  createInputStream,
  importFmcsaDataset,
  mapDatasetRow,
  normalizeDocketNumber,
  normalizeDotNumber,
  parseCsvLine,
  parseCsvRecords,
  parseFmcsaDate,
  stableStringify,
} from '../src/importers/fmcsaDataImporter';

async function collectRows(stream: Readable) {
  const rows: string[][] = [];

  for await (const row of parseCsvRecords(stream)) {
    rows.push(row);
  }

  return rows;
}

describe('FMCSA data importer', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('parses quoted CSV/TXT rows', () => {
    expect(parseCsvLine('"MC123456","00123456","","PROPERTY BROKER","GRANTED, PENDING","01/02/2024","","",""')).toEqual([
      'MC123456',
      '00123456',
      '',
      'PROPERTY BROKER',
      'GRANTED, PENDING',
      '01/02/2024',
      '',
      '',
      '',
    ]);
  });

  it('maps carrier fields correctly', () => {
    const row = mapDatasetRow('carrier', [
      'mc1557892',
      '4090101',
      '  QUIK X LLC ',
      '',
      'A',
      'I',
      'N',
      'N',
      'N',
      'Y',
      '75000',
      '',
      '',
      '123 Main St',
      'Chicago',
      'IL',
      'US',
      '60601',
      '3125550101',
      'PO Box 1',
      'Chicago',
      'IL',
      'US',
      '60602',
    ]);

    expect(row).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      legal_name: 'QUIK X LLC',
      dba_name: null,
      broker_stat: 'A',
      bus_state_code: 'IL',
      mail_zip_code: '60602',
    });
  });

  it('maps active insurance fields correctly', () => {
    const row = mapDatasetRow('active-insurance', [
      'MC1557892',
      '4090101',
      'BMC-84',
      'SURETY BOND',
      'AMERICAN ALTERNATIVE INSURANCE CORPORATION',
      '2024070128',
      '09/10/2024',
      '09/01/2024',
      '',
    ]);

    expect(row).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      ins_form_code: 'BMC-84',
      posted_date: '2024-09-10',
      effective_date: '2024-09-01',
      cancel_effective_date: null,
    });
  });

  it('maps insurance history fields correctly', () => {
    const row = mapDatasetRow('insurance-history', [
      'MC1557892',
      '4090101',
      'BMC-84',
      'REPLACED',
      'SURETY BOND',
      'OLD123',
      '01/15/2024',
      '09/01/2024',
      'REPLACED BY NEW POLICY',
      'OLD SURETY CO',
    ]);

    expect(row).toMatchObject({
      cancellation_method: 'REPLACED',
      policy_no: 'OLD123',
      effective_date: '2024-01-15',
      cancel_effective_date: '2024-09-01',
      insurance_company_name: 'OLD SURETY CO',
    });
  });

  it('maps revocation fields correctly', () => {
    const row = mapDatasetRow('revocation', [
      'MC1557892',
      '4090101',
      'BROKER',
      '10/01/2024',
      'INVOLUNTARY',
      '10/31/2024',
    ]);

    expect(row).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      authority_type: 'BROKER',
      serve_date: '2024-10-01',
      revocation_type: 'INVOLUNTARY',
      effective_date: '2024-10-31',
    });
  });

  it('maps authority history fields and derived flags correctly', () => {
    const row = mapDatasetRow('authority-history', [
      'mc1557892',
      '4090101',
      '',
      'PROPERTY BROKER',
      'REINSTATED',
      '01/02/2024',
      'DISCONTINUED REVOCATION',
      '02/03/2024',
      '02/04/2024',
    ]);

    expect(row).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      sub_number: null,
      original_action_date: '2024-01-02',
      final_decision_date: '2024-02-03',
      final_served_date: '2024-02-04',
      is_broker_authority: true,
      is_revoked: false,
      is_negative_final_action: false,
      is_reinstated: true,
      is_discontinued_revocation: true,
    });
  });

  it('normalizes DOT and MC values', () => {
    expect(normalizeDotNumber('DOT 123456')).toBe('00123456');
    expect(normalizeDocketNumber(' mc1557892 ')).toBe('MC1557892');
  });

  it('parses valid MM/DD/YYYY dates and rejects invalid dates', () => {
    expect(parseFmcsaDate('12/31/2025')).toBe('2025-12-31');
    expect(parseFmcsaDate('02/30/2025')).toBeNull();
    expect(parseFmcsaDate('')).toBeNull();
  });

  it('builds upsert SQL with expected conflict behavior', () => {
    const row = mapDatasetRow('active-insurance', [
      'MC1557892',
      '4090101',
      'BMC-84',
      'SURETY BOND',
      'SURETY CO',
      'POLICY1',
      '09/10/2024',
      '09/01/2024',
      '',
    ]);

    if (!row) {
      throw new Error('Expected active-insurance row to map.');
    }

    const batch = buildUpsertBatch('active-insurance', [row]);

    expect(batch.text).toContain('INSERT INTO fmcsa_active_pending_insurance');
    expect(batch.text).toContain('ON CONFLICT (source_record_hash)');
    expect(batch.text).toContain('DO UPDATE SET');
    expect(batch.text).toContain('raw_record = EXCLUDED.raw_record');
    expect(batch.text).toContain('updated_at = EXCLUDED.updated_at');
  });

  it('preserves rows with same selected columns but different hidden raw fields', () => {
    const headers = [
      'DOCKET_NUMBER',
      'DOT_NUMBER',
      'ins_form_code',
      'ins_type_desc',
      'name_company',
      'policy_no',
      'trans_date',
      'effective_date',
      'cancl_effective_date',
      'underl_lim_amount',
    ];
    const rowA = mapDatasetRow('active-insurance', [
      'MC1557892',
      '4090101',
      'BMC-84',
      'SURETY BOND',
      'SURETY CO',
      'POLICY1',
      '09/10/2024',
      '09/01/2024',
      '',
      '1000',
    ], headers);
    const rowB = mapDatasetRow('active-insurance', [
      'MC1557892',
      '4090101',
      'BMC-84',
      'SURETY BOND',
      'SURETY CO',
      'POLICY1',
      '09/10/2024',
      '09/01/2024',
      '',
      '2000',
    ], headers);

    expect(rowA).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      policy_no: 'POLICY1',
      effective_date: '2024-09-01',
    });
    expect(rowB).toMatchObject({
      docket_number: 'MC1557892',
      dot_number: '04090101',
      policy_no: 'POLICY1',
      effective_date: '2024-09-01',
    });
    expect(rowA?.source_record_hash).not.toBe(rowB?.source_record_hash);

    const batch = buildUpsertBatch('active-insurance', [rowA!, rowB!]);
    expect(batch.text).toContain('ON CONFLICT (source_record_hash)');
    expect(batch.values).toHaveLength(26);
  });

  it('creates the same source hash for the exact same raw row', () => {
    const rowA = mapDatasetRow('revocation', [
      'MC1557892',
      '4090101',
      'BROKER',
      '10/01/2024',
      'VOLUNTARY',
      '10/31/2024',
    ]);
    const rowB = mapDatasetRow('revocation', [
      'MC1557892',
      '4090101',
      'BROKER',
      '10/01/2024',
      'VOLUNTARY',
      '10/31/2024',
    ]);

    expect(rowA?.source_record_hash).toBe(rowB?.source_record_hash);
  });

  it('hash is stable regardless of raw object key order', () => {
    const rawA = { b: 'two', a: 'one', c: null };
    const rawB = { c: null, a: 'one', b: 'two' };

    expect(stableStringify(rawA)).toBe(stableStringify(rawB));
    expect(createSourceRecordHash(rawA)).toBe(createSourceRecordHash(rawB));
  });

  it('migration keeps lookup indexes by dot_number and docket_number', () => {
    const migration = fs.readFileSync(
      path.join(__dirname, '../migrations/001_create_fmcsa_source_tables.sql'),
      'utf8',
    );

    expect(migration).toContain('ON fmcsa_carriers (dot_number)');
    expect(migration).toContain('ON fmcsa_carriers (docket_number)');
    expect(migration).toContain('ON fmcsa_authority_history (dot_number)');
    expect(migration).toContain('ON fmcsa_authority_history (docket_number)');
  });

  it('imports from a local file stream', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-import-${Date.now()}.csv`);
    fs.writeFileSync(filePath, 'MC1557892,4090101,QUIK X LLC,,,,,,,,,,,,,,,,,,,,,\n');
    const pool = { query: jest.fn().mockResolvedValue({ rowCount: 1 }) };

    try {
      const stats = await importFmcsaDataset({
        datasetType: 'carrier',
        inputSource: filePath,
        pool,
        batchSize: 1,
      });

      expect(stats).toMatchObject({
        rowsRead: 1,
        rowsInsertedOrUpdated: 1,
        rowsFailed: 0,
        batches: 1,
      });
      expect(pool.query).toHaveBeenCalledTimes(1);
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('dedupes exact duplicate source records inside one batch', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-import-duplicate-${Date.now()}.csv`);
    fs.writeFileSync(
      filePath,
      [
        'DOCKET_NUMBER,DOT_NUMBER,TYPE_LICENSE,ORDER1_SERVE_DATE,ORDER2_TYPE_DESC,order2_effective_Date',
        'MC1557892,4090101,BROKER,10/01/2024,VOLUNTARY,10/31/2024',
        'MC1557892,4090101,BROKER,10/01/2024,VOLUNTARY,10/31/2024',
      ].join('\n'),
    );
    const pool = { query: jest.fn().mockResolvedValue({ rowCount: 1 }) };

    try {
      const stats = await importFmcsaDataset({
        datasetType: 'revocation',
        inputSource: filePath,
        pool,
        batchSize: 10,
      });

      expect(stats.rowsRead).toBe(2);
      expect(stats.rowsInsertedOrUpdated).toBe(1);
      expect(pool.query.mock.calls[0][1]).toHaveLength(10);
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('streams input from an HTTP URL', async () => {
    jest.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(Readable.toWeb(Readable.from(['MC1557892,4090101,BROKER,10/01/2024,VOLUNTARY,10/31/2024\n'])) as BodyInit),
    );

    const stream = await createInputStream('https://example.com/revocation.csv');
    const rows = await collectRows(stream);

    expect(rows).toEqual([
      ['MC1557892', '4090101', 'BROKER', '10/01/2024', 'VOLUNTARY', '10/31/2024'],
    ]);
  });

  it('streams input from an S3 URL', async () => {
    const s3Client = {
      send: jest.fn().mockResolvedValue({
        Body: Readable.from(['MC1557892,4090101,BROKER,10/01/2024,VOLUNTARY,10/31/2024\n']),
      }),
    };

    const stream = await createInputStream('s3://dispatch-ai-fmcsa/revocation.csv', {
      s3Client: s3Client as never,
    });
    const rows = await collectRows(stream);

    expect(s3Client.send).toHaveBeenCalledTimes(1);
    expect(rows).toEqual([
      ['MC1557892', '4090101', 'BROKER', '10/01/2024', 'VOLUNTARY', '10/31/2024'],
    ]);
  });
});
