import type { Readable } from 'stream';

export const AUTHORITY_HISTORY_BATCH_SIZE = 1000;

export const AUTHORITY_HISTORY_COLUMNS = [
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

const INSERT_COLUMNS = [
  ...AUTHORITY_HISTORY_COLUMNS,
  'is_broker_authority',
  'is_carrier_authority',
  'is_negative_final_action',
  'is_revoked',
  'is_reinstated',
  'is_discontinued_revocation',
  'raw_record',
] as const;

export type AuthorityHistoryColumn = (typeof AUTHORITY_HISTORY_COLUMNS)[number];

export type RawAuthorityHistoryRecord = Record<AuthorityHistoryColumn, string>;

export interface AuthorityHistoryImportRow {
  docket_number: string;
  dot_number: string;
  sub_number: string | null;
  authority_type: string | null;
  original_action: string | null;
  original_action_date: string | null;
  final_action: string | null;
  final_decision_date: string | null;
  final_served_date: string | null;
  is_broker_authority: boolean;
  is_carrier_authority: boolean;
  is_negative_final_action: boolean;
  is_revoked: boolean;
  is_reinstated: boolean;
  is_discontinued_revocation: boolean;
  raw_record: RawAuthorityHistoryRecord;
}

export interface InsertBatch {
  text: string;
  values: unknown[];
}

function normalizeNullable(value: string | undefined): string | null {
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

export function parseAuthorityHistoryDate(value: string | undefined): string | null {
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

  for await (const chunk of stream) {
    const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);

    for (const char of text) {
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
        if (char === '\r') {
          continue;
        }

        fields.push(field);
        yield fields;
        fields = [];
        field = '';
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

export function mapAuthorityHistoryRow(fields: string[]): AuthorityHistoryImportRow | null {
  const raw_record = AUTHORITY_HISTORY_COLUMNS.reduce((record, column, index) => {
    record[column] = fields[index] ?? '';
    return record;
  }, {} as RawAuthorityHistoryRecord);

  const docketNumber = normalizeDocketNumber(raw_record.docket_number);
  const dotNumber = normalizeDotNumber(raw_record.dot_number);

  if (!docketNumber || !dotNumber) {
    return null;
  }

  const authorityType = normalizeNullable(raw_record.authority_type);
  const originalAction = normalizeNullable(raw_record.original_action);
  const finalAction = normalizeNullable(raw_record.final_action);
  const authorityTypeUpper = authorityType?.toUpperCase() ?? '';
  const originalActionUpper = originalAction?.toUpperCase() ?? '';
  const finalActionUpper = finalAction?.toUpperCase() ?? '';
  const isDiscontinuedRevocation = finalActionUpper.includes('DISCONTINUED REVOCATION');
  const hasNegativeFinalAction =
    finalActionUpper.includes('REVOKED') ||
    finalActionUpper.includes('REVOCATION') ||
    finalActionUpper.includes('INACTIVATION') ||
    finalActionUpper.includes('SUSPENDED');

  return {
    docket_number: docketNumber,
    dot_number: dotNumber,
    sub_number: normalizeNullable(raw_record.sub_number),
    authority_type: authorityType,
    original_action: originalAction,
    original_action_date: parseAuthorityHistoryDate(raw_record.original_action_date),
    final_action: finalAction,
    final_decision_date: parseAuthorityHistoryDate(raw_record.final_decision_date),
    final_served_date: parseAuthorityHistoryDate(raw_record.final_served_date),
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
    raw_record,
  };
}

export function buildAuthorityHistoryInsertBatch(rows: AuthorityHistoryImportRow[]): InsertBatch {
  if (rows.length === 0) {
    throw new Error('Cannot build an insert batch with zero rows.');
  }

  const values: unknown[] = [];
  const tuples = rows.map((row) => {
    const placeholders = INSERT_COLUMNS.map((column) => {
      values.push(column === 'raw_record' ? JSON.stringify(row.raw_record) : row[column]);
      const placeholder = `$${values.length}`;
      return column === 'raw_record' ? `${placeholder}::jsonb` : placeholder;
    });

    return `(${placeholders.join(', ')})`;
  });

  return {
    text: `
      INSERT INTO fmcsa_authority_history (${INSERT_COLUMNS.join(', ')})
      VALUES ${tuples.join(', ')}
      ON CONFLICT DO NOTHING
    `,
    values,
  };
}
