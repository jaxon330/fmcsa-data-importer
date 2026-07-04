# Motus-native serving path

Implemented locally on 2026-06-21. Motus is the source of truth for the MVP broker-check path. The legacy `fmcsa_*` source tables remain unchanged as backup/reference.

## Datasets

| Dataset | Daily difference | All history |
|---|---|---|
| Carrier | `nakq-58th` | `inys-ebih` |
| Active insurance | `x96h-evps` | `c5y8-a4uz` |
| Insurance history | `xe5s-wca7` | `3uet-3z4i` |
| Revoke/suspend | `e67p-xyd5` | `wb4f-neki` |
| Authority history | `dm5j-zc6c` | `yu5v-wbh6` |

All Motus datasets use Socrata `rows.csv`, include headers, and are saved with a `.csv` extension.

## Serving tables

- `fmcsa_motus_carriers`
- `fmcsa_motus_active_insurance`
- `fmcsa_motus_insurance_history`
- `fmcsa_motus_revocations`
- `fmcsa_motus_authority_history`
- `fmcsa_import_jobs`

Each source row retains a non-null `raw_record`, source-record hash, and import-job lineage. Dockets retain the source value and a punctuation-insensitive canonical value. USDOT identifiers remain text without forced eight-digit padding.

## Commands

```bash
npm run db:migrate

npm run sync:fmcsa -- \
  --provider motus \
  --source allHist \
  --datasets active-insurance,insurance-history,revocation,authority-history \
  --force
```

Carrier all-history must not be run without explicit approval.

## Local validation

Local migration and non-carrier imports succeeded on 2026-06-21:

| Table | Rows |
|---|---:|
| `fmcsa_import_jobs` | 4 |
| `fmcsa_motus_carriers` | 0 |
| `fmcsa_motus_active_insurance` | 80,524 |
| `fmcsa_motus_insurance_history` | 20,741 |
| `fmcsa_motus_revocations` | 2,104 |
| `fmcsa_motus_authority_history` | 55,856 |

Importer results:

- Active insurance: 185,577 read, 80,524 inserted, 1 rejected.
- Insurance history: 32,660 read, 20,741 inserted, 0 rejected.
- Revoke/suspend: 2,245 read, 2,104 inserted, 12 rejected.
- Authority history: 55,875 read, 55,856 inserted, 8 rejected.

Exact duplicate rows are skipped by `source_record_hash`; rows missing required identifiers are rejected.

## Deployment status and limitation

- Carrier all-history was not run, so broker-check batch comparison is blocked until the Motus carrier baseline is approved and loaded.
- AWS was not deployed.
- Scheduler configuration was not changed.
