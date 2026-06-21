import fs from 'fs';
import crypto from 'crypto';
import { Readable } from 'stream';
import { GetObjectCommand, S3Client, type S3ClientConfig } from '@aws-sdk/client-s3';
import type { Pool } from 'pg';

export const DEFAULT_IMPORT_BATCH_SIZE = 1000;

export const DATASET_TYPES = [
  'carrier',
  'active-insurance',
  'insurance-history',
  'revocation',
  'authority-history',
] as const;

export type DatasetType = (typeof DATASET_TYPES)[number];
export type FmcsaSourceFormat = 'diff' | 'allHist' | 'motusDiff' | 'motusAllHist';

export interface DatasetConfig {
  datasetType: DatasetType;
  tableName: string;
  sourceColumns: readonly string[];
  sourceAliases?: Readonly<Record<string, readonly string[]>>;
  insertColumns: readonly string[];
  conflictColumns: readonly string[];
  dateColumns: readonly string[];
  requiredColumns: readonly string[];
  derive?: (row: Record<string, unknown>) => Record<string, unknown>;
}

export interface InsertBatch {
  text: string;
  values: unknown[];
}

export interface ImportStats {
  datasetType: DatasetType;
  sourceFormat: FmcsaSourceFormat;
  inputSource: string;
  rowsRead: number;
  rowsSkipped: number;
  rowsInsertedOrUpdated: number;
  rowsFailed: number;
  batches: number;
  durationMs: number;
}

export interface ImportOptions {
  datasetType: DatasetType;
  inputSource: string;
  pool: Pick<Pool, 'query'>;
  batchSize?: number;
  s3Client?: S3Client;
  sourceFormat?: FmcsaSourceFormat;
  skipRows?: number;
  progressEvery?: number;
}

export interface DryRunPreview {
  dataset: DatasetType;
  source: FmcsaSourceFormat;
  columnCount: number;
  preview: Array<Record<string, unknown>>;
}

interface SourceLayout {
  sourceColumns: readonly string[];
  columnIndexes: Readonly<Partial<Record<string, number>>>;
  expectedColumnCount?: number;
}

const COMMON_COLUMNS = ['source_record_hash', 'raw_record', 'imported_at', 'updated_at'] as const;

const AUTHORITY_HISTORY_DERIVED_COLUMNS = [
  'is_broker_authority',
  'is_carrier_authority',
  'is_negative_final_action',
  'is_revoked',
  'is_reinstated',
  'is_discontinued_revocation',
] as const;

const CARRIER_DIFF_COLUMNS = [
  'docket_number',
  'dot_number',
  'mx_type',
  'rfc_number',
  'common_stat',
  'contract_stat',
  'broker_stat',
  'common_app_pend',
  'contract_app_pend',
  'broker_app_pend',
  'common_rev_pend',
  'contract_rev_pend',
  'broker_rev_pend',
  'property_chk',
  'passenger_chk',
  'hhg_chk',
  'private_auth_chk',
  'enterprise_chk',
  'min_cov_amount',
  'cargo_req',
  'bond_req',
  'bipd_file',
  'cargo_file',
  'bond_file',
  'undeliverable_mail',
  'dba_name',
  'legal_name',
  'bus_street_po',
  'bus_colonia',
  'bus_city',
  'bus_state_code',
  'bus_ctry_code',
  'bus_zip_code',
  'bus_telno',
  'bus_fax',
  'mail_street_po',
  'mail_colonia',
  'mail_city',
  'mail_state_code',
  'mail_ctry_code',
  'mail_zip_code',
  'mail_telno',
  'mail_fax',
] as const;

const ACTIVE_INSURANCE_DIFF_COLUMNS = [
  'docket_number',
  'dot_number',
  'ins_form_code',
  'insurance_type_description',
  'insurance_company_name',
  'policy_no',
  'posted_date',
  'underl_lim_amount',
  'max_cov_amount',
  'effective_date',
  'cancel_effective_date',
] as const;

const INSURANCE_HISTORY_DIFF_COLUMNS = [
  'docket_number',
  'dot_number',
  'ins_form_code',
  'cancellation_method',
  'ins_cancl_form',
  'ins_type_ind',
  'insurance_type_description',
  'policy_no',
  'min_cov_amount',
  'ins_class_code',
  'effective_date',
  'underl_lim_amount',
  'max_cov_amount',
  'cancel_effective_date',
  'specific_cancellation_method',
  'inser_branch',
  'insurance_company_name',
] as const;

const REVOCATION_DIFF_COLUMNS = [
  'docket_number',
  'dot_number',
  'authority_type',
  'serve_date',
  'revocation_type',
  'effective_date',
] as const;

const AUTHORITY_HISTORY_DIFF_COLUMNS = [
  'docket_number',
  'dot_number',
  'sub_number',
  'authority_type',
  'original_action',
  'original_action_date',
  'final_action',
  'final_decision_date',
  'final_served_date',
] as const;

const DIFF_SOURCE_LAYOUTS: Record<DatasetType, SourceLayout> = {
  carrier: {
    sourceColumns: CARRIER_DIFF_COLUMNS,
    expectedColumnCount: 43,
    columnIndexes: {
      docket_number: 0,
      dot_number: 1,
      common_stat: 4,
      contract_stat: 5,
      broker_stat: 6,
      broker_app_pend: 9,
      broker_rev_pend: 12,
      bond_req: 20,
      bipd_file: 21,
      cargo_file: 22,
      bond_file: 23,
      dba_name: 25,
      legal_name: 26,
      bus_street_po: 27,
      bus_city: 29,
      bus_state_code: 30,
      bus_ctry_code: 31,
      bus_zip_code: 32,
      bus_telno: 33,
      mail_street_po: 35,
      mail_city: 37,
      mail_state_code: 38,
      mail_ctry_code: 39,
      mail_zip_code: 40,
    },
  },
  'active-insurance': {
    sourceColumns: ACTIVE_INSURANCE_DIFF_COLUMNS,
    expectedColumnCount: 11,
    columnIndexes: {
      docket_number: 0,
      dot_number: 1,
      ins_form_code: 2,
      insurance_type_description: 3,
      insurance_company_name: 4,
      policy_no: 5,
      posted_date: 6,
      effective_date: 9,
      cancel_effective_date: 10,
    },
  },
  'insurance-history': {
    sourceColumns: INSURANCE_HISTORY_DIFF_COLUMNS,
    expectedColumnCount: 17,
    columnIndexes: {
      docket_number: 0,
      dot_number: 1,
      ins_form_code: 2,
      cancellation_method: 3,
      insurance_type_description: 6,
      policy_no: 7,
      effective_date: 10,
      cancel_effective_date: 13,
      specific_cancellation_method: 14,
      insurance_company_name: 16,
    },
  },
  revocation: {
    sourceColumns: REVOCATION_DIFF_COLUMNS,
    expectedColumnCount: 6,
    columnIndexes: {
      docket_number: 0,
      dot_number: 1,
      authority_type: 2,
      serve_date: 3,
      revocation_type: 4,
      effective_date: 5,
    },
  },
  'authority-history': {
    sourceColumns: AUTHORITY_HISTORY_DIFF_COLUMNS,
    expectedColumnCount: 9,
    columnIndexes: {
      docket_number: 0,
      dot_number: 1,
      sub_number: 2,
      authority_type: 3,
      original_action: 4,
      original_action_date: 5,
      final_action: 6,
      final_decision_date: 7,
      final_served_date: 8,
    },
  },
};

export const DATASET_CONFIGS: Record<DatasetType, DatasetConfig> = {
  carrier: {
    datasetType: 'carrier',
    tableName: 'fmcsa_carriers',
    sourceColumns: [
      'docket_number',
      'dot_number',
      'legal_name',
      'dba_name',
      'broker_stat',
      'common_stat',
      'contract_stat',
      'broker_app_pend',
      'broker_rev_pend',
      'bond_req',
      'bond_file',
      'bipd_file',
      'cargo_file',
      'bus_street_po',
      'bus_city',
      'bus_state_code',
      'bus_ctry_code',
      'bus_zip_code',
      'bus_telno',
      'mail_street_po',
      'mail_city',
      'mail_state_code',
      'mail_ctry_code',
      'mail_zip_code',
    ],
    sourceAliases: {
      dot_number: ['dot_number', 'usdot_number', 'USDOT_NUMBER'],
      legal_name: ['legal_name'],
      dba_name: ['dba_name'],
      broker_stat: ['broker_stat'],
      common_stat: ['common_stat'],
      contract_stat: ['contract_stat'],
      broker_app_pend: ['broker_app_pend'],
      broker_rev_pend: ['broker_rev_pend'],
      bond_req: ['bond_req'],
      bond_file: ['bond_file'],
      bipd_file: ['bipd_file'],
      cargo_file: ['cargo_file'],
      bus_street_po: ['bus_street_po'],
      bus_city: ['bus_city'],
      bus_state_code: ['bus_state_code'],
      bus_ctry_code: ['bus_ctry_code'],
      bus_zip_code: ['bus_zip_code'],
      bus_telno: ['bus_telno'],
      mail_street_po: ['mail_street_po'],
      mail_city: ['mail_city'],
      mail_state_code: ['mail_state_code'],
      mail_ctry_code: ['mail_ctry_code'],
      mail_zip_code: ['mail_zip_code'],
    },
    insertColumns: [
      'docket_number',
      'dot_number',
      'legal_name',
      'dba_name',
      'broker_stat',
      'common_stat',
      'contract_stat',
      'broker_app_pend',
      'broker_rev_pend',
      'bond_req',
      'bond_file',
      'bipd_file',
      'cargo_file',
      'bus_street_po',
      'bus_city',
      'bus_state_code',
      'bus_ctry_code',
      'bus_zip_code',
      'bus_telno',
      'mail_street_po',
      'mail_city',
      'mail_state_code',
      'mail_ctry_code',
      'mail_zip_code',
      ...COMMON_COLUMNS,
    ],
    conflictColumns: ['source_record_hash'],
    dateColumns: [],
    requiredColumns: ['docket_number', 'dot_number'],
  },
  'active-insurance': {
    datasetType: 'active-insurance',
    tableName: 'fmcsa_active_pending_insurance',
    sourceColumns: [
      'docket_number',
      'dot_number',
      'ins_form_code',
      'insurance_type_description',
      'insurance_company_name',
      'policy_no',
      'posted_date',
      'effective_date',
      'cancel_effective_date',
    ],
    sourceAliases: {
      dot_number: ['dot_number', 'usdot_number', 'USDOT_NUMBER'],
      insurance_type_description: ['insurance_type_description', 'ins_type_desc', 'mod_col_1'],
      insurance_company_name: ['insurance_company_name', 'name_company'],
      posted_date: ['posted_date', 'trans_date'],
      cancel_effective_date: ['cancel_effective_date', 'cancl_effective_date'],
    },
    insertColumns: [
      'docket_number',
      'dot_number',
      'ins_form_code',
      'insurance_type_description',
      'insurance_company_name',
      'policy_no',
      'posted_date',
      'effective_date',
      'cancel_effective_date',
      ...COMMON_COLUMNS,
    ],
    conflictColumns: ['source_record_hash'],
    dateColumns: ['posted_date', 'effective_date', 'cancel_effective_date'],
    requiredColumns: ['docket_number', 'dot_number'],
  },
  'insurance-history': {
    datasetType: 'insurance-history',
    tableName: 'fmcsa_insurance_history',
    sourceColumns: [
      'docket_number',
      'dot_number',
      'ins_form_code',
      'cancellation_method',
      'insurance_type_description',
      'policy_no',
      'effective_date',
      'cancel_effective_date',
      'specific_cancellation_method',
      'insurance_company_name',
    ],
    sourceAliases: {
      dot_number: ['dot_number', 'usdot_number', 'USDOT_NUMBER'],
      cancellation_method: ['cancellation_method', 'cancl_method_gen', 'mod_col_1'],
      ins_form_code: ['ins_form_code'],
      insurance_type_description: ['insurance_type_description', 'ins_type_desc', 'mod_col_3'],
      cancel_effective_date: ['cancel_effective_date', 'cancl_effective_date'],
      specific_cancellation_method: ['specific_cancellation_method', 'cancl_method'],
      insurance_company_name: ['insurance_company_name', 'name_company'],
    },
    insertColumns: [
      'docket_number',
      'dot_number',
      'ins_form_code',
      'cancellation_method',
      'insurance_type_description',
      'policy_no',
      'effective_date',
      'cancel_effective_date',
      'specific_cancellation_method',
      'insurance_company_name',
      ...COMMON_COLUMNS,
    ],
    conflictColumns: ['source_record_hash'],
    dateColumns: ['effective_date', 'cancel_effective_date'],
    requiredColumns: ['docket_number', 'dot_number'],
  },
  revocation: {
    datasetType: 'revocation',
    tableName: 'fmcsa_revocations',
    sourceColumns: [
      'docket_number',
      'dot_number',
      'authority_type',
      'serve_date',
      'revocation_type',
      'effective_date',
    ],
    sourceAliases: {
      dot_number: ['dot_number', 'usdot_number', 'USDOT_NUMBER'],
      authority_type: ['authority_type', 'type_license', 'op_auth_type'],
      serve_date: ['serve_date', 'order1_serve_date'],
      revocation_type: ['revocation_type', 'order2_type_desc', 'order1_type_desc'],
      effective_date: ['effective_date', 'order2_effective_date', 'order1_effective_date'],
    },
    insertColumns: [
      'docket_number',
      'dot_number',
      'authority_type',
      'serve_date',
      'revocation_type',
      'effective_date',
      ...COMMON_COLUMNS,
    ],
    conflictColumns: ['source_record_hash'],
    dateColumns: ['serve_date', 'effective_date'],
    requiredColumns: ['docket_number', 'dot_number'],
  },
  'authority-history': {
    datasetType: 'authority-history',
    tableName: 'fmcsa_authority_history',
    sourceColumns: [
      'docket_number',
      'dot_number',
      'sub_number',
      'authority_type',
      'original_action',
      'original_action_date',
      'final_action',
      'final_decision_date',
      'final_served_date',
    ],
    sourceAliases: {
      dot_number: ['dot_number', 'usdot_number', 'USDOT_NUMBER'],
      authority_type: ['authority_type', 'op_auth_type', 'mod_col_1'],
      original_action: ['original_action', 'original_action_desc', 'reason'],
      original_action_date: ['original_action_date', 'orig_served_date'],
      final_action: ['final_action', 'disp_action_desc', 'op_auth_status'],
      final_decision_date: ['final_decision_date', 'disp_decided_date', 'status_change_date'],
      final_served_date: ['final_served_date', 'disp_served_date'],
    },
    insertColumns: [
      'docket_number',
      'dot_number',
      'sub_number',
      'authority_type',
      'original_action',
      'original_action_date',
      'final_action',
      'final_decision_date',
      'final_served_date',
      ...AUTHORITY_HISTORY_DERIVED_COLUMNS,
      ...COMMON_COLUMNS,
    ],
    conflictColumns: ['source_record_hash'],
    dateColumns: ['original_action_date', 'final_decision_date', 'final_served_date'],
    requiredColumns: ['docket_number', 'dot_number'],
    derive: deriveAuthorityHistoryFlags,
  },
};

export function parseDatasetType(value: string | undefined): DatasetType {
  if (DATASET_TYPES.includes(value as DatasetType)) {
    return value as DatasetType;
  }

  throw new Error(`Unsupported datasetType "${value ?? ''}". Supported values: ${DATASET_TYPES.join(', ')}`);
}

export function parseSourceFormat(value: string | undefined): FmcsaSourceFormat {
  if (value === 'diff' || value === 'allHist' || value === 'motusDiff' || value === 'motusAllHist') {
    return value;
  }

  throw new Error(`Unsupported source "${value ?? ''}". Supported source values: diff, allHist, motusDiff, motusAllHist`);
}

export function getBatchSize(value = process.env.FMCSA_IMPORT_BATCH_SIZE): number {
  if (!value) {
    return DEFAULT_IMPORT_BATCH_SIZE;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error('FMCSA_IMPORT_BATCH_SIZE must be a positive integer.');
  }

  return parsed;
}

export function parseNonNegativeInteger(value: string | undefined, name: string): number {
  if (!value) {
    return 0;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer.`);
  }

  return parsed;
}

export function normalizeNullable(value: string | undefined): string | null {
  const trimmed = value?.trim() ?? '';
  return trimmed === '' ? null : trimmed;
}

export function normalizeDotNumber(value: string | undefined): string | null {
  const digits = value?.replace(/\D/g, '') ?? '';
  return digits === '' ? null : digits.padStart(8, '0');
}

export function normalizeDocketNumber(value: string | undefined): string | null {
  const normalized = normalizeNullable(value);
  return normalized ? normalized.toUpperCase() : null;
}

export function parseFmcsaDate(value: string | undefined): string | null {
  const normalized = normalizeNullable(value);
  if (!normalized) {
    return null;
  }

  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(normalized);
  if (!match) {
    return null;
  }

  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));

  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  return [
    String(year).padStart(4, '0'),
    String(month).padStart(2, '0'),
    String(day).padStart(2, '0'),
  ].join('-');
}

export function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let field = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(field);
      field = '';
    } else {
      field += char;
    }
  }

  fields.push(field);
  return fields;
}

export async function* parseCsvRecords(stream: Readable): AsyncGenerator<string[]> {
  let fields: string[] = [];
  let field = '';
  let inQuotes = false;
  let pendingQuote = false;
  let previousWasCarriageReturn = false;

  for await (const chunk of stream) {
    const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);

    for (const char of text) {
      if (previousWasCarriageReturn) {
        previousWasCarriageReturn = false;
        if (char === '\n') {
          continue;
        }
      }

      if (pendingQuote) {
        if (char === '"') {
          field += '"';
          pendingQuote = false;
          continue;
        }

        inQuotes = false;
        pendingQuote = false;
      }

      if (char === '"') {
        if (inQuotes) {
          pendingQuote = true;
        } else {
          inQuotes = true;
        }
        continue;
      }

      if (char === ',' && !inQuotes) {
        fields.push(field);
        field = '';
        continue;
      }

      if ((char === '\n' || char === '\r') && !inQuotes) {
        fields.push(field);
        yield fields;
        fields = [];
        field = '';
        previousWasCarriageReturn = char === '\r';
        continue;
      }

      field += char;
    }
  }

  if (pendingQuote) {
    pendingQuote = false;
    inQuotes = false;
  }

  if (inQuotes) {
    throw new Error('Invalid CSV input: unterminated quoted field.');
  }

  if (field !== '' || fields.length > 0) {
    fields.push(field);
    yield fields;
  }
}

export function mapDatasetRow(
  datasetType: DatasetType,
  fields: string[],
  headers?: string[],
  sourceFormat: FmcsaSourceFormat = 'allHist',
): Record<string, unknown> | null {
  const config = DATASET_CONFIGS[datasetType];
  const layout = isFixedLayoutSource(sourceFormat) ? DIFF_SOURCE_LAYOUTS[datasetType] : null;
  const rawColumns = headers ?? layout?.sourceColumns ?? config.sourceColumns;
  const rawRecord = rawColumns.reduce<Record<string, string | null>>((record, column, index) => {
    record[normalizeHeaderName(column)] = normalizeNullable(fields[index]);
    return record;
  }, {});
  const headerIndex = headers ? buildHeaderIndex(headers) : null;

  const row: Record<string, unknown> = {};

  for (const column of config.sourceColumns) {
    const sourceValue = getSourceValue(config, column, fields, headerIndex, layout);

    if (column === 'dot_number') {
      row[column] = normalizeDotNumber(sourceValue);
    } else if (column === 'docket_number') {
      row[column] = normalizeDocketNumber(sourceValue);
    } else if (config.dateColumns.includes(column)) {
      row[column] = parseFmcsaDate(sourceValue);
    } else {
      row[column] = normalizeNullable(sourceValue);
    }
  }

  if (datasetType === 'carrier' && headerIndex?.has('op_auth_type')) {
    Object.assign(row, deriveMotusCarrierAuthorityFields(rawRecord));
  }

  for (const requiredColumn of config.requiredColumns) {
    if (!row[requiredColumn]) {
      return null;
    }
  }

  Object.assign(row, config.derive?.(row) ?? {});
  row.raw_record = rawRecord;
  row.source_record_hash = createSourceRecordHash(rawRecord);
  row.imported_at = new Date();
  row.updated_at = new Date();

  return row;
}

export function createSourceRecordHash(rawRecord: Record<string, unknown>): string {
  return crypto.createHash('sha256').update(stableStringify(rawRecord)).digest('hex');
}

export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }

  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(record[key])}`)
      .join(',')}}`;
  }

  return JSON.stringify(value);
}

export function buildUpsertBatch(datasetType: DatasetType, rows: Array<Record<string, unknown>>): InsertBatch {
  if (rows.length === 0) {
    throw new Error('Cannot build an upsert batch with zero rows.');
  }

  const config = DATASET_CONFIGS[datasetType];
  const values: unknown[] = [];
  const tuples = rows.map((row) => {
    const placeholders = config.insertColumns.map((column) => {
      const value = column === 'raw_record' ? JSON.stringify(row[column]) : row[column];
      values.push(value);
      const placeholder = `$${values.length}`;
      return column === 'raw_record' ? `${placeholder}::jsonb` : placeholder;
    });

    return `(${placeholders.join(', ')})`;
  });

  return {
    text: `
      INSERT INTO ${config.tableName} (${config.insertColumns.join(', ')})
      VALUES ${tuples.join(', ')}
      ON CONFLICT (${config.conflictColumns.join(', ')})
      DO NOTHING
    `,
    values,
  };
}

export async function createInputStream(
  inputSource: string,
  options: { s3Client?: S3Client; s3ClientConfig?: S3ClientConfig } = {},
): Promise<Readable> {
  if (/^https?:\/\//i.test(inputSource)) {
    const response = await fetch(inputSource);
    if (!response.ok || !response.body) {
      throw new Error(`HTTP input request failed with status ${response.status}: ${inputSource}`);
    }

    return Readable.fromWeb(response.body as Parameters<typeof Readable.fromWeb>[0]);
  }

  if (inputSource.startsWith('s3://')) {
    const { bucket, key } = parseS3Url(inputSource);
    const client = options.s3Client ?? new S3Client(options.s3ClientConfig ?? {});
    const result = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));

    if (!result.Body) {
      throw new Error(`S3 object has no body: ${inputSource}`);
    }

    return bodyToReadable(result.Body);
  }

  return fs.createReadStream(inputSource, { encoding: 'utf8' });
}

export async function importFmcsaDataset(options: ImportOptions): Promise<ImportStats> {
  const batchSize = options.batchSize ?? getBatchSize();
  const sourceFormat = options.sourceFormat ?? 'allHist';
  const skipRows = options.skipRows ?? 0;
  const progressEvery = options.progressEvery ?? 0;
  const stream = await createInputStream(options.inputSource, { s3Client: options.s3Client });
  const startedAt = process.hrtime.bigint();
  const stats: ImportStats = {
    datasetType: options.datasetType,
    sourceFormat,
    inputSource: options.inputSource,
    rowsRead: 0,
    rowsSkipped: 0,
    rowsInsertedOrUpdated: 0,
    rowsFailed: 0,
    batches: 0,
    durationMs: 0,
  };
  const pendingRows: Array<Record<string, unknown>> = [];
  let headers: string[] | undefined;
  let firstRecord = true;

  const flush = async () => {
    if (pendingRows.length === 0) {
      return;
    }

    const rowsToFlush = dedupeRowsByConflictKey(options.datasetType, pendingRows.splice(0));
    const rowCountFallback = rowsToFlush.length;
    const batch = buildUpsertBatch(options.datasetType, rowsToFlush);
    const result = await options.pool.query(batch.text, batch.values);
    stats.rowsInsertedOrUpdated += typeof result.rowCount === 'number' ? result.rowCount : rowCountFallback;
    stats.batches += 1;
  };

  for await (const fields of parseCsvRecords(stream)) {
    if (firstRecord) {
      firstRecord = false;
      if (isHeaderedSource(sourceFormat) && isHeaderRow(options.datasetType, fields)) {
        headers = fields.map((field) => field.trim());
        continue;
      }
      validateColumnCount(options.datasetType, sourceFormat, fields.length);
    } else if (!headers && isFixedLayoutSource(sourceFormat)) {
      validateColumnCount(options.datasetType, sourceFormat, fields.length);
    }

    try {
      stats.rowsRead += 1;
      if (stats.rowsRead <= skipRows) {
        stats.rowsSkipped += 1;
        if (progressEvery > 0 && stats.rowsRead % progressEvery === 0) {
          logImportProgress(stats, startedAt);
        }
        continue;
      }

      const row = mapDatasetRow(options.datasetType, fields, headers, sourceFormat);
      if (!row) {
        stats.rowsFailed += 1;
        continue;
      }

      pendingRows.push(row);
      if (pendingRows.length >= batchSize) {
        await flush();
      }

      if (progressEvery > 0 && stats.rowsRead % progressEvery === 0) {
        logImportProgress(stats, startedAt);
      }
    } catch {
      stats.rowsFailed += 1;
    }
  }

  await flush();
  stats.durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

  return stats;
}

function logImportProgress(stats: ImportStats, startedAt: bigint): void {
  const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
  const rowsPerSecond = durationMs > 0 ? stats.rowsRead / (durationMs / 1000) : 0;
  console.log(
    [
      `progress ${stats.datasetType}:`,
      `rows read=${stats.rowsRead}`,
      `skipped=${stats.rowsSkipped}`,
      `inserted=${stats.rowsInsertedOrUpdated}`,
      `failed=${stats.rowsFailed}`,
      `batches=${stats.batches}`,
      `rate=${rowsPerSecond.toFixed(0)} rows/s`,
    ].join(' '),
  );
}

export async function previewFmcsaDataset(options: {
  datasetType: DatasetType;
  inputSource: string;
  sourceFormat: FmcsaSourceFormat;
  limit?: number;
  s3Client?: S3Client;
}): Promise<DryRunPreview> {
  const stream = await createInputStream(options.inputSource, { s3Client: options.s3Client });
  const preview: Array<Record<string, unknown>> = [];
  let headers: string[] | undefined;
  let firstRecord = true;
  let columnCount = 0;
  const limit = options.limit ?? 3;

  for await (const fields of parseCsvRecords(stream)) {
    if (firstRecord) {
      firstRecord = false;
      columnCount = fields.length;
      if (isHeaderedSource(options.sourceFormat) && isHeaderRow(options.datasetType, fields)) {
        headers = fields.map((field) => field.trim());
        continue;
      }
      validateColumnCount(options.datasetType, options.sourceFormat, fields.length);
    } else if (!headers && isFixedLayoutSource(options.sourceFormat)) {
      validateColumnCount(options.datasetType, options.sourceFormat, fields.length);
    }

    const row = mapDatasetRow(options.datasetType, fields, headers, options.sourceFormat);
    if (row) {
      preview.push(stripImportMetadata(row));
    }

    if (preview.length >= limit) {
      break;
    }
  }

  return {
    dataset: options.datasetType,
    source: options.sourceFormat,
    columnCount,
    preview,
  };
}

function deriveAuthorityHistoryFlags(row: Record<string, unknown>): Record<string, unknown> {
  const authorityTypeUpper = String(row.authority_type ?? '').toUpperCase();
  const originalActionUpper = String(row.original_action ?? '').toUpperCase();
  const finalActionUpper = String(row.final_action ?? '').toUpperCase();
  const isDiscontinuedRevocation = finalActionUpper.includes('DISCONTINUED REVOCATION');
  const hasNegativeFinalAction =
    finalActionUpper.includes('REVOKED') ||
    finalActionUpper.includes('REVOCATION') ||
    finalActionUpper.includes('INACTIVATION') ||
    finalActionUpper.includes('SUSPENDED');

  return {
    is_broker_authority: authorityTypeUpper.includes('BROKER'),
    is_carrier_authority:
      authorityTypeUpper.includes('COMMON') ||
      authorityTypeUpper.includes('CONTRACT') ||
      authorityTypeUpper.includes('MOTOR PROPERTY'),
    is_negative_final_action: hasNegativeFinalAction && !isDiscontinuedRevocation,
    is_revoked:
      !isDiscontinuedRevocation &&
      (finalActionUpper.includes('REVOKED') || finalActionUpper.includes('REVOCATION')),
    is_reinstated: originalActionUpper.includes('REINSTATED'),
    is_discontinued_revocation: isDiscontinuedRevocation,
  };
}

function deriveMotusCarrierAuthorityFields(rawRecord: Record<string, string | null>): Record<string, unknown> {
  const authorityType = String(rawRecord.op_auth_type ?? '').toUpperCase();
  const authorityStatus = normalizeMotusAuthorityStatus(rawRecord.op_auth_status);
  const isPending = String(rawRecord.op_auth_status ?? '').toUpperCase().includes('PENDING');
  const derived: Record<string, unknown> = {};

  if (authorityType.includes('BROKER')) {
    derived.broker_stat = authorityStatus;
    if (isPending) {
      derived.broker_app_pend = 'Y';
    }
  } else if (authorityType.includes('CONTRACT')) {
    derived.contract_stat = authorityStatus;
  } else if (authorityType.includes('COMMON') || authorityType.includes('MOTOR CARRIER')) {
    derived.common_stat = authorityStatus;
  }

  return derived;
}

function normalizeMotusAuthorityStatus(value: string | null | undefined): string | null {
  const normalized = normalizeNullable(value ?? undefined);
  if (!normalized) {
    return null;
  }

  const upper = normalized.toUpperCase();
  if (upper === 'ACTIVE') {
    return 'A';
  }
  if (upper === 'INACTIVE') {
    return 'I';
  }

  return normalized;
}

function normalizeHeaderName(value: string): string {
  return value.trim().toLowerCase();
}

function buildHeaderIndex(headers: string[]): Map<string, number> {
  const index = new Map<string, number>();
  headers.forEach((header, position) => {
    index.set(normalizeHeaderName(header), position);
  });
  return index;
}

function getColumnAliases(config: DatasetConfig, column: string): readonly string[] {
  return [column, ...(config.sourceAliases?.[column] ?? [])];
}

function getSourceValue(
  config: DatasetConfig,
  column: string,
  fields: string[],
  headerIndex: Map<string, number> | null,
  layout: SourceLayout | null,
): string | undefined {
  if (!headerIndex) {
    if (layout) {
      const index = layout.columnIndexes[column];
      return index === undefined ? undefined : fields[index];
    }

    return fields[config.sourceColumns.indexOf(column)];
  }

  for (const alias of getColumnAliases(config, column)) {
    const index = headerIndex.get(normalizeHeaderName(alias));
    if (index !== undefined) {
      return fields[index];
    }
  }

  return undefined;
}

function isHeaderRow(datasetType: DatasetType, fields: string[]): boolean {
  const config = DATASET_CONFIGS[datasetType];
  const headerIndex = buildHeaderIndex(fields);

  return config.requiredColumns.every((column) =>
    getColumnAliases(config, column).some((alias) => headerIndex.has(normalizeHeaderName(alias))),
  );
}

function validateColumnCount(
  datasetType: DatasetType,
  sourceFormat: FmcsaSourceFormat,
  columnCount: number,
): void {
  if (!isFixedLayoutSource(sourceFormat)) {
    return;
  }

  const expectedColumnCount = DIFF_SOURCE_LAYOUTS[datasetType].expectedColumnCount;
  if (expectedColumnCount === undefined || columnCount === expectedColumnCount) {
    return;
  }

  throw new Error(
    `Unexpected ${datasetType} ${sourceFormat} column count: got ${columnCount}, expected ${expectedColumnCount}. Run with --dry-run to preview the file before importing.`,
  );
}

function isFixedLayoutSource(sourceFormat: FmcsaSourceFormat): boolean {
  return sourceFormat === 'diff' || sourceFormat === 'motusDiff' || sourceFormat === 'motusAllHist';
}

function isHeaderedSource(sourceFormat: FmcsaSourceFormat): boolean {
  return sourceFormat === 'allHist' || sourceFormat === 'motusDiff';
}

function stripImportMetadata(row: Record<string, unknown>): Record<string, unknown> {
  const { imported_at, updated_at, source_record_hash, raw_record, ...previewRow } = row;
  void imported_at;
  void updated_at;
  void source_record_hash;
  void raw_record;
  return previewRow;
}

function dedupeRowsByConflictKey(
  datasetType: DatasetType,
  rows: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
  const config = DATASET_CONFIGS[datasetType];
  const rowsByKey = new Map<string, Record<string, unknown>>();

  for (const row of rows) {
    const key = JSON.stringify(config.conflictColumns.map((column) => row[column] ?? null));
    rowsByKey.set(key, row);
  }

  return [...rowsByKey.values()];
}

function parseS3Url(inputSource: string): { bucket: string; key: string } {
  const withoutScheme = inputSource.slice('s3://'.length);
  const slashIndex = withoutScheme.indexOf('/');
  if (slashIndex <= 0 || slashIndex === withoutScheme.length - 1) {
    throw new Error(`Invalid S3 URL: ${inputSource}`);
  }

  return {
    bucket: withoutScheme.slice(0, slashIndex),
    key: withoutScheme.slice(slashIndex + 1),
  };
}

function bodyToReadable(body: unknown): Readable {
  if (body instanceof Readable) {
    return body;
  }

  if (body && typeof (body as { transformToWebStream?: unknown }).transformToWebStream === 'function') {
    const webStream = (body as { transformToWebStream: () => unknown }).transformToWebStream();
    return Readable.fromWeb(webStream as Parameters<typeof Readable.fromWeb>[0]);
  }

  if (body && Symbol.asyncIterator in Object(body)) {
    return Readable.from(body as AsyncIterable<Uint8Array>);
  }

  throw new Error('Unsupported S3 body stream type.');
}
