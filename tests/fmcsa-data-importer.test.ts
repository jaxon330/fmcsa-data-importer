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
  previewFmcsaDataset,
  stableStringify,
} from '../src/importers/fmcsaDataImporter';
import {
  parseDatasetList,
  selectBatchFiles,
} from '../src/scripts/importFmcsaBatch';
import {
  downloadToFile,
} from '../src/scripts/downloadFmcsaFiles';

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

  it('maps active-insurance diff dates from indexes 9 and 10', () => {
    const row = mapDatasetRow('active-insurance', [
      'MC1808911',
      '04553798',
      '91X',
      'BIPD/Primary',
      'NATIONAL FIRE & MARINE INSURANCE CO.',
      '72TRS136977',
      '05/11/2026',
      '0',
      '300',
      '05/09/2027',
      '06/13/2027',
    ], undefined, 'diff');

    expect(row).toMatchObject({
      posted_date: '2026-05-11',
      effective_date: '2027-05-09',
      cancel_effective_date: '2027-06-13',
    });
  });

  it('does not use active-insurance diff index 7 as a date', () => {
    const row = mapDatasetRow('active-insurance', [
      'MC1700301',
      '04349172',
      '91X',
      'BIPD/Primary',
      'UNITED FINANCIAL CASUALTY COMPANY',
      'CA872530460',
      '05/14/2026',
      '07/04/2026',
      '1500',
      '07/16/2026',
      '',
    ], undefined, 'diff');

    expect(row?.effective_date).toBe('2026-07-16');
    expect(row?.effective_date).not.toBe('2026-07-04');
    expect(row?.cancel_effective_date).toBeNull();
  });

  it('maps carrier diff legal name from the daily diff layout', async () => {
    const filePath = path.join(os.tmpdir(), `carrier-diff-preview-${Date.now()}.txt`);
    fs.writeFileSync(
      filePath,
      '"MC000675","00124159"," ","","I","I","N","N","N","N","N","N","N","N","Y","N","N","N","05000","N","N","01500","N","N","Y","","A&M TRANSIT LINES, LLC","170 EAST PROSPECT STREET","","ALLIANCE","OH","US","44601","3308233124","3308232100","","","","","","","",""\n',
    );

    try {
      const preview = await previewFmcsaDataset({
        datasetType: 'carrier',
        inputSource: filePath,
        sourceFormat: 'diff',
      });

      expect(preview.columnCount).toBe(43);
      expect(preview.preview[0]).toMatchObject({
        docket_number: 'MC000675',
        dot_number: '00124159',
        legal_name: 'A&M TRANSIT LINES, LLC',
        broker_stat: 'N',
        bond_file: 'N',
      });
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('maps insurance-history diff policy, company, and date fields correctly', async () => {
    const filePath = path.join(os.tmpdir(), `insurance-history-diff-preview-${Date.now()}.txt`);
    fs.writeFileSync(
      filePath,
      '"MC191611","00280055","91","Cancelled","35"," ","BIPD","T-0201715-DM",1000,"","04/16/1987","","","04/16/1995","CANCEL","00","NATIONAL AMERICAN INSURANCE CO. OF NEW YORK"\n',
    );

    try {
      const preview = await previewFmcsaDataset({
        datasetType: 'insurance-history',
        inputSource: filePath,
        sourceFormat: 'diff',
      });

      expect(preview.columnCount).toBe(17);
      expect(preview.preview[0]).toMatchObject({
        policy_no: 'T-0201715-DM',
        insurance_company_name: 'NATIONAL AMERICAN INSURANCE CO. OF NEW YORK',
        effective_date: '1987-04-16',
        cancel_effective_date: '1995-04-16',
        cancellation_method: 'Cancelled',
      });
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('fails fast on wrong diff column count before inserting', async () => {
    const filePath = path.join(os.tmpdir(), `active-insurance-bad-diff-${Date.now()}.txt`);
    fs.writeFileSync(filePath, 'MC1,123,91X,BIPD\n');
    const pool = { query: jest.fn() };

    try {
      await expect(importFmcsaDataset({
        datasetType: 'active-insurance',
        inputSource: filePath,
        sourceFormat: 'diff',
        pool,
      })).rejects.toThrow('Unexpected active-insurance diff column count');
      expect(pool.query).not.toHaveBeenCalled();
    } finally {
      fs.unlinkSync(filePath);
    }
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
    expect(batch.text).toContain('DO NOTHING');
    expect(batch.text).not.toContain('DO UPDATE SET');
    expect(batch.text).not.toContain('raw_record = EXCLUDED.raw_record');
    expect(batch.text).not.toContain('updated_at = EXCLUDED.updated_at');
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
        sourceFormat: 'allHist',
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

  it('skips already processed data rows before inserting', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-import-skip-${Date.now()}.csv`);
    fs.writeFileSync(
      filePath,
      [
        'DOCKET_NUMBER,DOT_NUMBER,ins_form_code,ins_type_desc,name_company,policy_no,trans_date,underl_lim_amount,max_cov_amount,effective_date,cancl_effective_date',
        'MC1,1,91X,BIPD,INS CO,POL1,01/01/2024,0,750,01/02/2024,',
        'MC2,2,91X,BIPD,INS CO,POL2,01/01/2024,0,750,01/03/2024,',
        'MC3,3,91X,BIPD,INS CO,POL3,01/01/2024,0,750,01/04/2024,',
      ].join('\n'),
    );
    const pool = { query: jest.fn().mockResolvedValue({ rowCount: 1 }) };

    try {
      const stats = await importFmcsaDataset({
        datasetType: 'active-insurance',
        inputSource: filePath,
        sourceFormat: 'allHist',
        pool,
        batchSize: 10,
        skipRows: 2,
      });

      expect(stats.rowsRead).toBe(3);
      expect(stats.rowsSkipped).toBe(2);
      expect(stats.rowsInsertedOrUpdated).toBe(1);
      expect(pool.query).toHaveBeenCalledTimes(1);
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('batch file selection only processes requested datasets and uses latest dated files', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-batch-'));
    const files = [
      'carrier_2026_05_29.txt',
      'carrier_2026_05_30.txt',
      'actpendins_2026_05_30.txt',
      'inshist_2026_05_30.txt',
      'authhist_2026_05_30.txt',
    ];

    try {
      for (const file of files) {
        fs.writeFileSync(path.join(dir, file), '');
      }

      const selected = selectBatchFiles(dir, 'diff', parseDatasetList('carrier,active-insurance,insurance-history'));

      expect(selected.map((file) => file.datasetType)).toEqual([
        'carrier',
        'active-insurance',
        'insurance-history',
      ]);
      expect(selected.map((file) => path.basename(file.filePath))).toEqual([
        'carrier_2026_05_30.txt',
        'actpendins_2026_05_30.txt',
        'inshist_2026_05_30.txt',
      ]);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('batch import selection supports broker-check v1 datasets without revocation or authority-history', () => {
    const datasets = parseDatasetList('carrier,active-insurance,insurance-history');

    expect(datasets).toEqual(['carrier', 'active-insurance', 'insurance-history']);
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

  it('retries transient FMCSA download failures and succeeds', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-download-retry-${Date.now()}.txt`);
    const fetchMock = jest.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 503, statusText: 'Service Temporarily Unavailable' }))
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from(['downloaded'])) as BodyInit));
    const warnMock = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

    try {
      await downloadToFile('https://data.transportation.gov/api/views/example/rows.csv', filePath, {
        datasetName: 'carrier',
        source: 'diff',
        retryBaseDelayMs: 0,
        sleep: async () => undefined,
        random: () => 0,
      });

      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(fs.readFileSync(filePath, 'utf8')).toBe('downloaded');
      expect(warnMock.mock.calls[0].join(' ')).toContain('HTTP 503');
      expect(warnMock.mock.calls[0].join(' ')).toContain('data.transportation.gov/api/views/example/rows.csv');
    } finally {
      fs.rmSync(filePath, { force: true });
      fs.rmSync(`${filePath}.tmp`, { force: true });
    }
  });

  it('fails after max retries for transient FMCSA download failures', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-download-fail-${Date.now()}.txt`);
    const fetchMock = jest.spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(null, { status: 503, statusText: 'Service Temporarily Unavailable' }));
    jest.spyOn(console, 'warn').mockImplementation(() => undefined);

    try {
      await expect(downloadToFile('https://data.transportation.gov/api/views/example/rows.csv', filePath, {
        datasetName: 'carrier',
        source: 'diff',
        maxAttempts: 3,
        retryBaseDelayMs: 0,
        sleep: async () => undefined,
        random: () => 0,
      })).rejects.toThrow('Download failed: 503 Service Temporarily Unavailable');

      expect(fetchMock).toHaveBeenCalledTimes(3);
      expect(fs.existsSync(filePath)).toBe(false);
    } finally {
      fs.rmSync(filePath, { force: true });
      fs.rmSync(`${filePath}.tmp`, { force: true });
    }
  });

  it('does not retry non-transient FMCSA download failures', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-download-non-retry-${Date.now()}.txt`);
    const fetchMock = jest.spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(null, { status: 401, statusText: 'Unauthorized' }));

    try {
      await expect(downloadToFile('https://data.transportation.gov/api/views/example/rows.csv', filePath, {
        datasetName: 'carrier',
        source: 'diff',
        sleep: async () => undefined,
      })).rejects.toThrow('Download failed: 401 Unauthorized');

      expect(fetchMock).toHaveBeenCalledTimes(1);
    } finally {
      fs.rmSync(filePath, { force: true });
      fs.rmSync(`${filePath}.tmp`, { force: true });
    }
  });

  it('does not log Socrata app tokens during retry logging', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-download-token-log-${Date.now()}.txt`);
    const token = 'secret-socrata-token';
    jest.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 503, statusText: 'Service Temporarily Unavailable' }))
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from(['downloaded'])) as BodyInit));
    const warnMock = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

    try {
      await downloadToFile('https://data.transportation.gov/api/views/example/export.csv?app_token=should-not-appear', filePath, {
        headers: { 'X-App-Token': token },
        datasetName: 'carrier',
        source: 'diff',
        retryBaseDelayMs: 0,
        sleep: async () => undefined,
        random: () => 0,
      });

      const logs = warnMock.mock.calls.flat().join('\n');
      expect(logs).not.toContain(token);
      expect(logs).not.toContain('should-not-appear');
      expect(logs).toContain('data.transportation.gov/api/views/example/export.csv');
    } finally {
      fs.rmSync(filePath, { force: true });
      fs.rmSync(`${filePath}.tmp`, { force: true });
    }
  });
});
