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
  downloadFmcsaFiles,
  downloadToFile,
} from '../src/scripts/downloadFmcsaFiles';
import {
  buildProcessedFileIdentityRef,
  getFmcsaRawStorageConfig,
  markProcessedFileIdentity,
} from '../src/storage/fmcsaRawStorage';

async function collectRows(stream: Readable) {
  const rows: string[][] = [];

  for await (const row of parseCsvRecords(stream)) {
    rows.push(row);
  }

  return rows;
}

describe('FMCSA data importer', () => {
  const originalEnv = process.env;

  afterEach(() => {
    jest.restoreAllMocks();
    process.env = originalEnv;
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

  it('downloads a Motus all-history CSV export', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-motus-download-'));
    const body = 'DOCKET_NUMBER,USDOT_NUMBER,OP_AUTH_TYPE,OP_AUTH_STATUS\nMC012892,2217388,Broker of Property (Except Household Goods),Inactive\n';
    const fetchMock = jest.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(Readable.toWeb(Readable.from([body])) as BodyInit, {
        headers: {
          ETag: '"motus-carrier-etag"',
          'Last-Modified': 'Sun, 14 Jun 2026 10:00:00 GMT',
          'Content-Length': String(body.length),
        },
      }),
    );
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env = { ...originalEnv, FMCSA_STORAGE_TYPE: 'local' };

    try {
      const results = await downloadFmcsaFiles({
        provider: 'motus',
        downloadMode: 'allHist',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-14T12:00:00Z'),
      });

      expect(fetchMock).toHaveBeenCalledWith(
        'https://data.transportation.gov/api/views/inys-ebih/rows.csv?accessType=DOWNLOAD',
        expect.any(Object),
      );
      expect(results[0]).toEqual(expect.objectContaining({
        datasetKey: 'carrier',
        skipped: false,
        failed: false,
      }));
      expect(results[0].downloadIdentity).toMatchObject({
        etag: '"motus-carrier-etag"',
        lastModified: 'Sun, 14 Jun 2026 10:00:00 GMT',
        contentLength: body.length,
      });
      expect(fs.existsSync(path.join(dir, 'motusAllHist', 'motus_carrier_all_with_history_2026_06_14.csv'))).toBe(true);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('parses Motus all-history as headered CSV', async () => {
    const filePath = path.join(os.tmpdir(), `motus-carrier-preview-${Date.now()}.csv`);
    fs.writeFileSync(
      filePath,
      [
        'DOCKET_NUMBER,USDOT_NUMBER,OP_AUTH_TYPE,OP_AUTH_STATUS,BOND_REQ,BOND_FILE,DBA_NAME,LEGAL_NAME',
        'MC012892,2217388,Broker of Property (Except Household Goods),Inactive,Y,N,N C BRINKE,NORMAN CHARLES BRINKE',
      ].join('\n'),
    );

    try {
      const preview = await previewFmcsaDataset({
        datasetType: 'carrier',
        inputSource: filePath,
        sourceFormat: 'motusAllHist',
      });

      expect(preview.columnCount).toBe(8);
      expect(preview.preview[0]).toMatchObject({
        docket_number: 'MC012892',
        dot_number: '02217388',
        legal_name: 'NORMAN CHARLES BRINKE',
        dba_name: 'N C BRINKE',
        broker_stat: 'I',
        bond_req: 'Y',
      });
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('downloads a Motus daily diff from Socrata rows.csv', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-motus-diff-download-'));
    const body = [
      'DOCKET_NUMBER,USDOT_NUMBER,RFC_NUMBER,OP_AUTH_TYPE,OP_AUTH_STATUS,MIN_COV_AMOUNT,CARGO_REQ,BOND_REQ,BIPD_FILE,CARGO_FILE,BOND_FILE,BUS_UNDELIVERABLE_MAIL,MAIL_UNDELIVERABLE_MAIL,DBA_NAME,LEGAL_NAME,BUS_STREET_PO,BUS_COLONIA,BUS_CITY,BUS_STATE_CODE,BUS_CTRY_CODE,BUS_ZIP_CODE,BUS_TELNO,MAIL_STREET_PO,MAIL_COLONIA,MAIL_CITY,MAIL_STATE_CODE,MAIL_CTRY_CODE,MAIL_ZIP_CODE',
      'MC86415293,7388268,,Motor Carrier of Property (Except Household Goods),Active,750000,N,N,750000,N,N,Y,Y,,TEST CARRIER,1 MAIN ST,,CHICAGO,IL,US,60601,,1 MAIN ST,,CHICAGO,IL,US,60601',
    ].join('\n');
    const fetchMock = jest.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(Readable.toWeb(Readable.from([body])) as BodyInit, {
        headers: {
          ETag: '"motus-carrier-diff-etag"',
          'Last-Modified': 'Mon, 15 Jun 2026 11:12:54 GMT',
          'Content-Length': String(body.length),
        },
      }),
    );
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env = { ...originalEnv, FMCSA_STORAGE_TYPE: 'local' };

    try {
      const results = await downloadFmcsaFiles({
        provider: 'motus',
        downloadMode: 'diff',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-15T12:00:00Z'),
      });

      expect(fetchMock).toHaveBeenCalledWith(
        'https://data.transportation.gov/api/views/nakq-58th/rows.csv?accessType=DOWNLOAD',
        expect.any(Object),
      );
      expect(results[0]).toEqual(expect.objectContaining({
        datasetKey: 'carrier',
        skipped: false,
        failed: false,
      }));
      expect(fs.existsSync(path.join(dir, 'motusDiff', 'motus_carrier_2026_06_15.csv'))).toBe(true);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('maps Motus carrier daily diff headers into compatible carrier fields', async () => {
    const filePath = path.join(os.tmpdir(), `motus-carrier-diff-preview-${Date.now()}.csv`);
    fs.writeFileSync(
      filePath,
      [
        'DOCKET_NUMBER,USDOT_NUMBER,RFC_NUMBER,OP_AUTH_TYPE,OP_AUTH_STATUS,MIN_COV_AMOUNT,CARGO_REQ,BOND_REQ,BIPD_FILE,CARGO_FILE,BOND_FILE,BUS_UNDELIVERABLE_MAIL,MAIL_UNDELIVERABLE_MAIL,DBA_NAME,LEGAL_NAME,BUS_STREET_PO,BUS_COLONIA,BUS_CITY,BUS_STATE_CODE,BUS_CTRY_CODE,BUS_ZIP_CODE,BUS_TELNO,MAIL_STREET_PO,MAIL_COLONIA,MAIL_CITY,MAIL_STATE_CODE,MAIL_CTRY_CODE,MAIL_ZIP_CODE',
        'MC86415293,7388268,,Motor Carrier of Property (Except Household Goods),Active,750000,N,N,750000,N,N,Y,Y,,TEST CARRIER,1 MAIN ST,,CHICAGO,IL,US,60601,,1 MAIN ST,,CHICAGO,IL,US,60601',
        'MC999999,1234567,,Broker,Inactive,0,N,Y,0,N,Y,N,N,TEST DBA,TEST BROKER,2 MAIN ST,,DALLAS,TX,US,75001,,2 MAIN ST,,DALLAS,TX,US,75001',
      ].join('\n'),
    );

    try {
      const preview = await previewFmcsaDataset({
        datasetType: 'carrier',
        inputSource: filePath,
        sourceFormat: 'motusDiff',
        limit: 2,
      });

      expect(preview.columnCount).toBe(28);
      expect(preview.preview).toHaveLength(1);
      expect(preview.preview[0]).toMatchObject({
        docket_number: 'MC999999',
        dot_number: '01234567',
        dba_name: 'TEST DBA',
        broker_stat: 'I',
        bond_file: 'Y',
      });
    } finally {
      fs.unlinkSync(filePath);
    }
  });

  it('maps Motus carrier fields from headers', () => {
    const headers = [
      'DOCKET_NUMBER', 'USDOT_NUMBER', 'OP_AUTH_TYPE', 'OP_AUTH_STATUS',
      'BOND_REQ', 'BOND_FILE', 'DBA_NAME', 'LEGAL_NAME',
    ];
    const row = mapDatasetRow('carrier', [
      'MC012892', '02217388', 'Broker of Property (Except Household Goods)', 'Inactive',
      'Y', 'N', 'N C BRINKE', 'NORMAN CHARLES BRINKE',
    ], headers, 'motusAllHist');

    expect(row).toMatchObject({
      docket_number: 'MC012892',
      dot_number: '02217388',
      legal_name: 'NORMAN CHARLES BRINKE',
      dba_name: 'N C BRINKE',
      broker_stat: 'I',
      bond_req: 'Y',
      bond_file: 'N',
    });
  });

  it('maps Motus active-insurance fields from headers', () => {
    const headers = [
      'DOCKET_NUMBER', 'USDOT_NUMBER', 'INS_FORM_CODE', 'INS_TYPE_CODE',
      'POLICY_NO', 'EFFECTIVE_DATE', 'INSURANCE_COMPANY_NAME', 'TRANS_DATE',
    ];
    const row = mapDatasetRow('active-insurance', [
      'MC1572973', '04100741', '91X', '1', 'CA972204680', '20320810',
      'PROGRESSIVE MOUNTAIN INSURANCE COMPANY OF OHI', '20230814',
    ], headers, 'motusAllHist');

    expect(row).toMatchObject({
      docket_number: 'MC1572973',
      dot_number: '04100741',
      ins_form_code: '91X',
      insurance_type_description: '1',
      insurance_company_name: 'PROGRESSIVE MOUNTAIN INSURANCE COMPANY OF OHI',
      policy_no: 'CA972204680',
      posted_date: '2023-08-14',
      effective_date: '2032-08-10',
      cancel_effective_date: null,
    });
  });

  it('maps Motus insurance-history fields from headers', () => {
    const headers = [
      'DOCKET_NUMBER', 'USDOT_NUMBER', 'INS_FORM_CODE', 'FILING_STATUS_REASON',
      'INS_TYPE_DESC', 'POLICY_NO', 'EFFECTIVE_DATE', 'CANCL_EFFECTIVE_DATE',
      'INSURANCE_COMPANY_NAME',
    ];
    const row = mapDatasetRow('insurance-history', [
      'FF000031', '00000000', '91', 'Cancelled', 'BIPD', 'TP9458318',
      '19860101', '20040923', 'NATIONAL FIRE INSURANCE CO.',
    ], headers, 'motusAllHist');

    expect(row).toMatchObject({
      docket_number: 'FF000031',
      dot_number: '00000000',
      ins_form_code: '91',
      cancellation_method: 'Cancelled',
      insurance_type_description: 'BIPD',
      policy_no: 'TP9458318',
      effective_date: '1986-01-01',
      cancel_effective_date: '2004-09-23',
      specific_cancellation_method: null,
      insurance_company_name: 'NATIONAL FIRE INSURANCE CO.',
    });
  });

  it('maps Motus revocation fields from headers', () => {
    const headers = [
      'DOCKET_NUMBER', 'USDOT_NUMBER', 'OP_AUTH_TYPE',
      'ORDER1_SERVE_DATE', 'ORDER1_TYPE_DESC', 'ORDER1_EFFECTIVE_DATE',
    ];
    const row = mapDatasetRow('revocation', [
      'MX255345', '00000000', 'COMMON', '20051122', 'INVOLUNTARY REVOCATION', '20051227',
    ], headers, 'motusAllHist');

    expect(row).toMatchObject({
      docket_number: 'MX255345',
      dot_number: '00000000',
      authority_type: 'COMMON',
      serve_date: '2005-11-22',
      revocation_type: 'INVOLUNTARY REVOCATION',
      effective_date: '2005-12-27',
    });
  });

  it('maps Motus authority-history fields from headers', () => {
    const headers = [
      'DOCKET_NUMBER', 'USDOT_NUMBER', 'OP_AUTH_TYPE',
      'OP_AUTH_STATUS', 'REASON', 'STATUS_CHANGE_DATE',
    ];
    const row = mapDatasetRow('authority-history', [
      'MC000647', '00085526', 'Motor Carrier of Property (Except Household Goods)',
      'Inactive', 'REVOKED', '19920227',
    ], headers, 'motusAllHist');

    expect(row).toMatchObject({
      docket_number: 'MC000647',
      dot_number: '00085526',
      sub_number: null,
      authority_type: 'Motor Carrier of Property (Except Household Goods)',
      original_action: 'REVOKED',
      original_action_date: null,
      final_action: 'Inactive',
      final_decision_date: '1992-02-27',
      final_served_date: null,
      is_carrier_authority: true,
      is_revoked: false,
    });
  });

  it('skips a Motus file with the same processed identity', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-motus-processed-'));
    const body = 'DOCKET_NUMBER,USDOT_NUMBER,OP_AUTH_TYPE,OP_AUTH_STATUS\nMC012892,2217388,Broker of Property (Except Household Goods),Inactive\n';
    const responseHeaders = {
      ETag: '"same-motus-file"',
      'Last-Modified': 'Sun, 14 Jun 2026 10:00:00 GMT',
      'Content-Length': String(body.length),
    };
    jest.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from([body])) as BodyInit, { headers: responseHeaders }))
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from([body])) as BodyInit, { headers: responseHeaders }));
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env = { ...originalEnv, FMCSA_STORAGE_TYPE: 'local' };

    try {
      const first = await downloadFmcsaFiles({
        provider: 'motus',
        downloadMode: 'allHist',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-14T12:00:00Z'),
      });
      expect(first[0].failed).toBe(false);
      expect(first[0].skipped).toBe(false);

      const storage = getFmcsaRawStorageConfig(dir);
      const processedRef = buildProcessedFileIdentityRef(storage, 'motusAllHist', 'carrier', first[0].downloadIdentity!);
      await markProcessedFileIdentity(processedRef, storage, { test: true });

      const second = await downloadFmcsaFiles({
        provider: 'motus',
        downloadMode: 'allHist',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-15T12:00:00Z'),
      });

      expect(second[0]).toEqual(expect.objectContaining({
        datasetKey: 'carrier',
        skipped: true,
        skippedReason: 'already_processed',
        failed: false,
      }));
      expect(fs.existsSync(path.join(dir, 'motusAllHist', 'motus_carrier_all_with_history_2026_06_15.csv'))).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('fails fast on Motus column drift before inserting', async () => {
    const filePath = path.join(os.tmpdir(), `motus-active-insurance-bad-${Date.now()}.txt`);
    fs.writeFileSync(filePath, '"MC1572973","04100741","91X","BIPD/Primary"\n');
    const pool = { query: jest.fn() };

    try {
      await expect(importFmcsaDataset({
        datasetType: 'active-insurance',
        inputSource: filePath,
        sourceFormat: 'motusAllHist',
        pool,
      })).rejects.toThrow('Expected a header row for active-insurance motusAllHist');
      expect(pool.query).not.toHaveBeenCalled();
    } finally {
      fs.unlinkSync(filePath);
    }
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
    expect(parseFmcsaDate('20251231')).toBe('2025-12-31');
    expect(parseFmcsaDate('2025-12-31T00:00:00.000')).toBe('2025-12-31');
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

  it('builds Motus upserts against current serving tables by canonical key', () => {
    const row = mapDatasetRow(
      'active-insurance',
      ['MC1557892', '4090101', 'BMC-84', '4', 'POLICY1', '20240901', 'SURETY CO', '20240910'],
      [
        'DOCKET_NUMBER',
        'USDOT_NUMBER',
        'INS_FORM_CODE',
        'INS_TYPE_CODE',
        'POLICY_NO',
        'EFFECTIVE_DATE',
        'INSURANCE_COMPANY_NAME',
        'TRANS_DATE',
      ],
      'motusAllHist',
    );

    if (!row) {
      throw new Error('Expected Motus active-insurance row to map.');
    }

    const batch = buildUpsertBatch('active-insurance', [row], 'motusAllHist');

    expect(batch.text).toContain('INSERT INTO fmcsa_current_active_pending_insurance');
    expect(batch.text).toContain('ON CONFLICT (canonical_key)');
    expect(batch.text).toContain('DO UPDATE SET');
    expect(batch.text).toContain('source_priority <= EXCLUDED.source_priority');
    expect(row).toMatchObject({
      canonical_key: 'MC1557892|04090101|BMC-84|POLICY1|2024-09-01',
      source_provider: 'motus',
      source_priority: 100,
      cancel_effective_date: null,
    });
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

  it('skips an unpublished daily diff file on 404 without retrying or failing', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-diff-404-'));
    const fetchMock = jest.spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(null, { status: 404, statusText: 'Not Found' }));
    const logMock = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env = { ...originalEnv, FMCSA_STORAGE_TYPE: 'local' };

    try {
      const results = await downloadFmcsaFiles({
        downloadMode: 'diff',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-14T12:00:00Z'),
      });

      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(results).toEqual([
        expect.objectContaining({
          datasetKey: 'carrier',
          skipped: true,
          skippedReason: 'not_published',
          failed: false,
          error: 'Download failed: 404 Not Found',
        }),
      ]);
      expect(logMock.mock.calls.flat().join('\n')).toContain('daily diff file not published yet');
      expect(fs.existsSync(path.join(dir, 'diff', 'carrier_2026_06_14.txt'))).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
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

  it('downloads a daily diff file and captures file identity metadata', async () => {
    const filePath = path.join(os.tmpdir(), `fmcsa-download-identity-${Date.now()}.txt`);
    jest.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(Readable.toWeb(Readable.from(['downloaded'])) as BodyInit, {
        headers: {
          ETag: '"daily-diff-etag"',
          'Last-Modified': 'Sun, 14 Jun 2026 10:00:00 GMT',
          'Content-Length': '10',
        },
      }),
    );

    try {
      const result = await downloadToFile('https://data.transportation.gov/download/6qg9-x4f8/application/octet-stream', filePath, {
        datasetName: 'carrier',
        source: 'diff',
      });

      expect(fs.readFileSync(filePath, 'utf8')).toBe('downloaded');
      expect(result.identity).toMatchObject({
        etag: '"daily-diff-etag"',
        lastModified: 'Sun, 14 Jun 2026 10:00:00 GMT',
        contentLength: 10,
      });
      expect(result.identity.sha256).toMatch(/^[a-f0-9]{64}$/);
    } finally {
      fs.rmSync(filePath, { force: true });
      fs.rmSync(`${filePath}.tmp`, { force: true });
    }
  });

  it('skips a daily diff file with the same processed ETag/hash identity', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fmcsa-diff-processed-'));
    const body = 'downloaded';
    const firstResponseHeaders = {
      ETag: '"same-daily-diff"',
      'Last-Modified': 'Sun, 14 Jun 2026 10:00:00 GMT',
      'Content-Length': String(body.length),
    };
    const secondResponseHeaders = {
      ETag: '"same-daily-diff"',
      'Last-Modified': 'Sun, 14 Jun 2026 10:00:00 GMT',
      'Content-Length': String(body.length),
    };
    jest.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from([body])) as BodyInit, { headers: firstResponseHeaders }))
      .mockResolvedValueOnce(new Response(Readable.toWeb(Readable.from([body])) as BodyInit, { headers: secondResponseHeaders }));
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    process.env = { ...originalEnv, FMCSA_STORAGE_TYPE: 'local' };

    try {
      const first = await downloadFmcsaFiles({
        downloadMode: 'diff',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-14T12:00:00Z'),
      });
      expect(first[0].failed).toBe(false);
      expect(first[0].skipped).toBe(false);
      expect(first[0].downloadIdentity?.etag).toBe('"same-daily-diff"');

      const storage = getFmcsaRawStorageConfig(dir);
      const processedRef = buildProcessedFileIdentityRef(storage, 'diff', 'carrier', first[0].downloadIdentity!);
      await markProcessedFileIdentity(processedRef, storage, { test: true });

      const second = await downloadFmcsaFiles({
        downloadMode: 'diff',
        datasetKeys: ['carrier'],
        dir,
        date: new Date('2026-06-15T12:00:00Z'),
      });

      expect(second[0]).toEqual(expect.objectContaining({
        datasetKey: 'carrier',
        skipped: true,
        skippedReason: 'already_processed',
        failed: false,
      }));
      expect(fs.existsSync(path.join(dir, 'diff', 'carrier_2026_06_15.txt'))).toBe(false);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
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
