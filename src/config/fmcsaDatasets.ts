export type FmcsaDownloadMode = 'diff' | 'allHist';
export type FmcsaProvider = 'legacy' | 'motus';
export type FmcsaRawSource = FmcsaDownloadMode | 'motusDiff' | 'motusAllHist';

export type FmcsaDatasetKey =
  | 'carrier'
  | 'activeInsurance'
  | 'insurance'
  | 'insuranceHistory'
  | 'revocation'
  | 'authorityHistory';

export type FmcsaDatasetName =
  | 'carrier'
  | 'active-insurance'
  | 'insurance'
  | 'insurance-history'
  | 'revocation'
  | 'authority-history';

export interface FmcsaDatasetDownloadConfig {
  datasetId: string;
  filePrefix: string;
  extension: string;
}

export const FMCSA_BASE_DOWNLOAD_URL = 'https://data.transportation.gov/api/views';
export const FMCSA_ASSET_DOWNLOAD_URL = 'https://data.transportation.gov/download';
export const FMCSA_SODA3_DOWNLOAD_URL = 'https://data.transportation.gov/api/v3/views';

export const BROKER_CHECK_V1_DATASETS: FmcsaDatasetName[] = [
  'carrier',
  'active-insurance',
  'insurance-history',
];

export const DATASET_KEY_TO_NAME: Record<FmcsaDatasetKey, FmcsaDatasetName> = {
  carrier: 'carrier',
  activeInsurance: 'active-insurance',
  insurance: 'insurance',
  insuranceHistory: 'insurance-history',
  revocation: 'revocation',
  authorityHistory: 'authority-history',
};

export const DATASET_NAME_TO_KEY: Record<FmcsaDatasetName, FmcsaDatasetKey> = {
  carrier: 'carrier',
  'active-insurance': 'activeInsurance',
  insurance: 'insurance',
  'insurance-history': 'insuranceHistory',
  revocation: 'revocation',
  'authority-history': 'authorityHistory',
};

export const DATASET_KEY_ALIASES: Record<string, FmcsaDatasetKey> = {
  carrier: 'carrier',
  activeInsurance: 'activeInsurance',
  'active-insurance': 'activeInsurance',
  insurance: 'insurance',
  insuranceHistory: 'insuranceHistory',
  'insurance-history': 'insuranceHistory',
  revocation: 'revocation',
  authorityHistory: 'authorityHistory',
  'authority-history': 'authorityHistory',
};

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
  Record<Exclude<FmcsaDatasetKey, 'insurance'>, FmcsaDatasetDownloadConfig>
>;

export const MOTUS_DATASETS = {
  diff: {
    carrier: {
      datasetId: 'nakq-58th',
      filePrefix: 'motus_carrier',
      extension: 'csv',
    },
    activeInsurance: {
      datasetId: 'x96h-evps',
      filePrefix: 'motus_actpendins',
      extension: 'csv',
    },
    insuranceHistory: {
      datasetId: 'xe5s-wca7',
      filePrefix: 'motus_inshist',
      extension: 'csv',
    },
    revocation: {
      datasetId: 'e67p-xyd5',
      filePrefix: 'motus_revocation',
      extension: 'csv',
    },
    authorityHistory: {
      datasetId: 'dm5j-zc6c',
      filePrefix: 'motus_authhist',
      extension: 'csv',
    },
  },
  allHist: {
    carrier: {
      datasetId: 'inys-ebih',
      filePrefix: 'motus_carrier_all_with_history',
      extension: 'csv',
    },
    activeInsurance: {
      datasetId: 'c5y8-a4uz',
      filePrefix: 'motus_active_pending_insurance_all_with_history',
      extension: 'csv',
    },
    insurance: {
      datasetId: 'c5y8-a4uz',
      filePrefix: 'motus_insurance_all_with_history',
      extension: 'csv',
    },
    insuranceHistory: {
      datasetId: '3uet-3z4i',
      filePrefix: 'motus_insurance_history_all_with_history',
      extension: 'csv',
    },
    revocation: {
      datasetId: 'wb4f-neki',
      filePrefix: 'motus_revocation_all_with_history',
      extension: 'csv',
    },
    authorityHistory: {
      datasetId: 'yu5v-wbh6',
      filePrefix: 'motus_authority_history_all_with_history',
      extension: 'csv',
    },
  },
} as const satisfies {
  diff: Record<Exclude<FmcsaDatasetKey, 'insurance'>, FmcsaDatasetDownloadConfig>;
  allHist: Record<FmcsaDatasetKey, FmcsaDatasetDownloadConfig>;
};

export function buildFmcsaDownloadUrl(datasetId: string): string {
  return `${FMCSA_ASSET_DOWNLOAD_URL}/${datasetId}/application/octet-stream`;
}

export function parseFmcsaDatasetKeys(value: string | undefined, defaultDatasets?: FmcsaDatasetName[]): FmcsaDatasetKey[] {
  const rawDatasetNames = value
    ? value.split(',').map((rawDatasetKey) => rawDatasetKey.trim()).filter(Boolean)
    : defaultDatasets ?? [];

  if (rawDatasetNames.length === 0) {
    throw new Error(`--datasets requires a comma-separated dataset list. Supported values: ${supportedDatasetNames().join(', ')}`);
  }

  const seen = new Set<FmcsaDatasetKey>();
  return rawDatasetNames.map((rawDatasetKey) => {
    const datasetKey = DATASET_KEY_ALIASES[rawDatasetKey];
    if (!datasetKey) {
      throw new Error(`Unsupported dataset "${rawDatasetKey}". Supported values: ${supportedDatasetNames().join(', ')}`);
    }

    if (seen.has(datasetKey)) {
      throw new Error(`Duplicate dataset "${rawDatasetKey}"`);
    }
    seen.add(datasetKey);

    return datasetKey;
  });
}

export function datasetKeyToName(datasetKey: FmcsaDatasetKey): FmcsaDatasetName {
  return DATASET_KEY_TO_NAME[datasetKey];
}

export function supportedDatasetNames(): FmcsaDatasetName[] {
  return ['carrier', 'active-insurance', 'insurance', 'insurance-history', 'revocation', 'authority-history'];
}

export function buildFmcsaSodaExportUrl(datasetId: string): string {
  const url = new URL(`${FMCSA_SODA3_DOWNLOAD_URL}/${datasetId}/export.csv`);
  url.searchParams.set('accessType', 'DOWNLOAD');

  return url.toString();
}

export function buildMotusRowsCsvDownloadUrl(datasetId: string): string {
  const url = new URL(`${FMCSA_BASE_DOWNLOAD_URL}/${datasetId}/rows.csv`);
  url.searchParams.set('accessType', 'DOWNLOAD');

  return url.toString();
}

export function toRawSource(provider: FmcsaProvider, downloadMode: FmcsaDownloadMode): FmcsaRawSource {
  if (provider === 'legacy') {
    return downloadMode;
  }

  return downloadMode === 'diff' ? 'motusDiff' : 'motusAllHist';
}
