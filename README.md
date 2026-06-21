# FMCSA Data Importer

Standalone streaming importer for FMCSA source datasets.

Legacy imports remain in the `fmcsa_*` source tables. Motus imports are normalized into
legacy-compatible `fmcsa_current_*` serving tables used by broker-check.

## Supported Datasets

```bash
npm run import:fmcsa -- carrier /path/to/carrier_all_with_history.csv --source allHist
npm run import:fmcsa -- active-insurance /path/to/active_pending_insurance_all_with_history.csv --source allHist
npm run import:fmcsa -- insurance-history /path/to/insurance_history_all_with_history.csv --source allHist
npm run import:fmcsa -- revocation /path/to/revocation_all_with_history.csv --source allHist
npm run import:fmcsa -- authority-history /path/to/authority_history_all_with_history.csv --source allHist
```

Inputs can be:

- local file path
- HTTP/HTTPS URL
- S3 URL, for example `s3://dispatch-ai-fmcsa/carrier_all_with_history.csv`

The importer streams input and does not load the full file into memory.

## Environment

```dotenv
DATABASE_URL=postgresql://jakhongirmasaidov@localhost:5432/dispatch_local
FMCSA_IMPORT_BATCH_SIZE=1000
AWS_REGION=us-east-1
```

`FMCSA_IMPORT_BATCH_SIZE` defaults to `1000`.

## Commands

Install dependencies:

```bash
npm install
```

Run migrations:

```bash
npm run db:migrate
```

Run tests:

```bash
npm test
```

Build TypeScript:

```bash
npm run build
```

## Download FMCSA Source Files

Daily diff:

```bash
npm run download:fmcsa -- --download diff
npm run download:fmcsa -- --download diff --datasets carrier,active-insurance,insurance-history
```

All with history:

```bash
npm run download:fmcsa -- --download allHist
npm run download:fmcsa -- --download allHist --datasets active-insurance
```

Force overwrite existing dated files:

```bash
npm run download:fmcsa -- --download diff --force
```

All-history CSV exports use the Socrata v3 export API. Set `FMCSA_SOCRATA_APP_TOKEN`
or `SOCRATA_APP_TOKEN` if DOT requires request identification for CSV export.

Output:

```text
data/raw/diff/
data/raw/allHist/
```

When `FMCSA_STORAGE_TYPE=s3`, downloaded raw files are uploaded to:

```text
{FMCSA_S3_PREFIX}/{source}/{filename}
```

Example:

```text
fmcsa/raw/diff/carrier_2026_06_07.txt
```

## Sync Command

The production sync command runs download, file validation, import, and summary logging.

Broker-check v1 defaults to `carrier,active-insurance,insurance-history` when `--datasets` is omitted.

```bash
npm run sync:fmcsa -- --source diff
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history
npm run sync:fmcsa -- --source allHist --datasets carrier,active-insurance,insurance-history
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history --dry-run
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history --force
```

Initial current-table rebuild (legacy baseline followed by Motus All History overlay):

```bash
npm run sync:fmcsa -- \
  --provider motus \
  --source allHist \
  --datasets carrier,active-insurance,insurance-history,revocation,authority-history \
  --rebuild-current
```

Daily Motus overlay:

```bash
npm run sync:fmcsa -- \
  --provider motus \
  --source diff \
  --datasets carrier,active-insurance,insurance-history,revocation,authority-history
```

Optional local directory override:

```bash
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history --dir /app/data/raw
```

## Tables

Legacy imports write to:

- `fmcsa_carriers`
- `fmcsa_active_pending_insurance`
- `fmcsa_insurance_history`
- `fmcsa_revocations`
- `fmcsa_authority_history`

The migration also creates current serving tables:

- `fmcsa_current_carriers`
- `fmcsa_current_active_pending_insurance`
- `fmcsa_current_insurance_history`
- `fmcsa_current_revocations`
- `fmcsa_current_authority_history`

Broker-check reads the first three current tables. Motus rows never write to the legacy
tables. Current rows include `source_provider`, `source_priority`,
`last_legacy_seen_at`, and `last_motus_seen_at` for debugging.

Every table includes:

- selected normalized columns for lookup/querying
- `raw_record JSONB`
- `source_record_hash TEXT NOT NULL`
- `imported_at`
- `updated_at`

## Uniqueness Strategy

`source_record_hash` is the true uniqueness key.

FMCSA rows are not guaranteed to be unique by our selected business columns. Two source rows can have the same MC/DOT, dates, policy, or authority fields while differing in a source column we do not store as a normal column.

The importer computes:

```text
source_record_hash = sha256(JSON.stringify(normalizedRawRecord))
```

Where `normalizedRawRecord`:

- includes all source columns from the CSV row
- trims strings
- converts empty strings to `null`
- uses stable key ordering before hashing

The DB uses:

```sql
UNIQUE(source_record_hash)
```

MC/DOT columns are lookup keys, not uniqueness keys. Indexes are kept on `dot_number` and `docket_number`.

## Initial Import Notes

These timings were observed on the local Postgres import of full-history datasets.

```text
active-insurance
source rows:        467,983
final DB rows:      467,932
runtime:            15.47s

revocation
source rows:        ~1,529,083
final DB rows:      1,511,969
runtime:            interrupted earlier, but completed enough to load table

carrier
source rows:        1,860,604
final DB rows:      1,860,603
runtime:            140.06s

insurance-history
source rows:        7,427,776
final DB rows:      7,346,321
runtime:            1268.61s, about 21.1 minutes

authority-history
source rows:        4,941,925
final DB rows:      4,931,415
runtime:            834.14s, about 13.9 minutes
```

Long-running commands:

```bash
npm run import:fmcsa -- carrier /Users/jakhongirmasaidov/Projects/IT_projects/Dispatch_AI/Data_Transportation/carrier_all_with_history.csv
npm run import:fmcsa -- insurance-history /Users/jakhongirmasaidov/Projects/IT_projects/Dispatch_AI/Data_Transportation/insurance_history_all_with_history.csv
npm run import:fmcsa -- authority-history /Users/jakhongirmasaidov/Projects/IT_projects/Dispatch_AI/Data_Transportation/authority_history_all_with_history.csv
```

They ran long because these were initial full-history imports. Each row was streamed, parsed, normalized, hashed with SHA-256, upserted into Postgres, and indexed. For large tables, disk writes and index maintenance dominate runtime.

Remote DB imports may take similar or longer for the first full import, depending on DB CPU, disk I/O, network latency, and index performance.

Future daily imports should be much faster because they should use daily-difference files instead of the full-history files. The same importer upserts by `source_record_hash`, so unchanged rows do not duplicate, and only new or changed source rows are inserted or updated.

## Daily Diff Imports

Daily diff files use explicit no-header positional layouts. Always pass `--source diff` and run
`--dry-run` before importing new daily files.

```bash
npm run import:fmcsa -- carrier /path/to/carrier_2026_05_30.txt --source diff --dry-run
npm run import:fmcsa -- active-insurance /path/to/actpendins_2026_05_30.txt --source diff --dry-run
npm run import:fmcsa -- insurance-history /path/to/inshist_2026_05_30.txt --source diff --dry-run
```

Batch dry-run for broker-check v1 datasets:

```bash
npm run import:fmcsa:batch -- --source diff --datasets carrier,active-insurance,insurance-history --dir /path/to/data/raw/diff --dry-run
```

Batch import after preview confirms the mapped fields:

```bash
npm run import:fmcsa:batch -- --source diff --datasets carrier,active-insurance,insurance-history --dir /path/to/data/raw/diff
```

## Production Deployment

Required legacy/source DB tables:

- `fmcsa_carriers`
- `fmcsa_active_pending_insurance`
- `fmcsa_insurance_history`
- `fmcsa_revocations`
- `fmcsa_authority_history`

Required current serving DB tables:

- `fmcsa_current_carriers`
- `fmcsa_current_active_pending_insurance`
- `fmcsa_current_insurance_history`
- `fmcsa_current_revocations`
- `fmcsa_current_authority_history`

Broker-check v1 reads only the first three current serving tables.

Local environment:

```dotenv
DATABASE_URL=postgresql://...
FMCSA_STORAGE_TYPE=local
FMCSA_LOCAL_RAW_DATA_DIR=/app/data/raw
FMCSA_SOCRATA_APP_TOKEN=
LOG_LEVEL=INFO
```

Production S3 environment:

```dotenv
DATABASE_URL=postgresql://...
FMCSA_STORAGE_TYPE=s3
FMCSA_S3_BUCKET_NAME=dispatch-ai-fmcsa-raw-data
FMCSA_S3_PREFIX=fmcsa/raw
FMCSA_SOCRATA_APP_TOKEN=...
LOG_LEVEL=INFO
```

Manual run:

```bash
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history
```

Dry run:

```bash
npm run sync:fmcsa -- --source diff --datasets carrier,active-insurance,insurance-history --dry-run
```

Suggested schedule:

- Daily diff: 6:00 AM America/Chicago.
- Retry: 9:00 AM America/Chicago.
- All-history: manual or monthly, not daily.

Production AWS recommendation:

```text
EventBridge Scheduler
  -> ECS Fargate scheduled task
  -> fmcsa-data-importer container
  -> download daily diff to S3
  -> import/update Postgres
  -> logs to CloudWatch
```

Use AWS Secrets Manager or SSM Parameter Store for:

- `DATABASE_URL`
- `FMCSA_SOCRATA_APP_TOKEN`

AWS setup notes are in [docs/aws-scheduled-sync.md](/Users/jakhongirmasaidov/Projects/IT_projects/Dispatch_AI/fmcsa-data-importer/docs/aws-scheduled-sync.md).

## Legacy Production Flow

1. Download full-history FMCSA files to S3 once.
2. Run this importer against each S3 file.
3. Download daily-difference FMCSA files to S3.
4. Run the same importer with the same dataset type.
5. Let Postgres upsert by `source_record_hash`.

Example:

```bash
npm run import:fmcsa -- carrier s3://dispatch-ai-fmcsa/carrier_all_with_history.csv
```
