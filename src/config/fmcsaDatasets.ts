export type FmcsaDownloadMode = 'diff' | 'allHist';

export type FmcsaDatasetKey =
  | 'carrier'
  | 'activeInsurance'
  | 'insuranceHistory'
  | 'revocation'
  | 'authorityHistory';

export interface FmcsaDatasetDownloadConfig {
  datasetId: string;
  filePrefix: string;
  extension: string;
}

export const FMCSA_BASE_DOWNLOAD_URL = 'https://data.transportation.gov/api/views';
export const FMCSA_ASSET_DOWNLOAD_URL = 'https://data.transportation.gov/download';
export const FMCSA_SODA3_DOWNLOAD_URL = 'https://data.transportation.gov/api/v3/views';

export const FMCSA_DATASETS = {
  diff: {
    carrier: {
      datasetId: '6qg9-x4f8',
      filePrefix: 'carrier',
      extension: 'txt',
    },
    activeInsurance: {
      datasetId: 'chgs-tx6x',
      filePrefix: 'actpendins',
      extension: 'txt',
    },
    insuranceHistory: {
      datasetId: 'xkmg-ff2t',
      filePrefix: 'inshist',
      extension: 'txt',
    },
    revocation: {
      datasetId: 'pivg-szje',
      filePrefix: 'revocation',
      extension: 'txt',
    },
    authorityHistory: {
      datasetId: 'sn3k-dnx7',
      filePrefix: 'authhist',
      extension: 'txt',
    },
  },
  allHist: {
    carrier: {
      datasetId: '6eyk-hxee',
      filePrefix: 'carrier_all_with_history',
      extension: 'csv',
    },
    activeInsurance: {
      datasetId: 'qh9u-swkp',
      filePrefix: 'active_pending_insurance_all_with_history',
      extension: 'csv',
    },
    insuranceHistory: {
      datasetId: '6sqe-dvqs',
      filePrefix: 'insurance_history_all_with_history',
      extension: 'csv',
    },
    revocation: {
      datasetId: 'sa6p-acbp',
      filePrefix: 'revocation_all_with_history',
      extension: 'csv',
    },
    authorityHistory: {
      datasetId: '9mw4-x3tu',
      filePrefix: 'authority_history_all_with_history',
      extension: 'csv',
    },
  },
} as const satisfies Record<
  FmcsaDownloadMode,
  Record<FmcsaDatasetKey, FmcsaDatasetDownloadConfig>
>;

export function buildFmcsaDownloadUrl(datasetId: string): string {
  return `${FMCSA_ASSET_DOWNLOAD_URL}/${datasetId}/application/octet-stream`;
}

export function buildFmcsaSodaExportUrl(datasetId: string, appToken?: string): string {
  const url = new URL(`${FMCSA_SODA3_DOWNLOAD_URL}/${datasetId}/export.csv`);
  url.searchParams.set('accessType', 'DOWNLOAD');

  if (appToken) {
    url.searchParams.set('app_token', appToken);
  }

  return url.toString();
}
