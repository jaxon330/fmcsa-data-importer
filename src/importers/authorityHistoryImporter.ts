import {
  DEFAULT_IMPORT_BATCH_SIZE,
  buildUpsertBatch,
  mapDatasetRow,
  parseFmcsaDate,
} from './fmcsaDataImporter';

export {
  parseCsvLine,
  parseCsvRecords,
  normalizeDocketNumber,
  normalizeDotNumber,
} from './fmcsaDataImporter';

export const AUTHORITY_HISTORY_BATCH_SIZE = DEFAULT_IMPORT_BATCH_SIZE;
export const parseAuthorityHistoryDate = parseFmcsaDate;

export function mapAuthorityHistoryRow(fields: string[]) {
  return mapDatasetRow('authority-history', fields);
}

export function buildAuthorityHistoryInsertBatch(rows: Array<Record<string, unknown>>) {
  return buildUpsertBatch('authority-history', rows);
}
