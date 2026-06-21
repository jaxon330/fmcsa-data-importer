# Motus-native schema discovery for MVP broker-check

Date of discovery: 2026-06-21

Status: validated design source. The local implementation is documented in
`docs/motus-native-implementation.md`. AWS and scheduler configuration remain unchanged.

## Executive recommendation

Create separate Motus-native tables rather than loading Motus rows into the legacy-shaped tables.

The Motus model is materially different:

- Carrier rows represent individual operating authorities (OA), not one flattened carrier record with separate `broker_stat`, `common_stat`, and `contract_stat` columns.
- Motus replaces the legacy authority-status columns with `OP_AUTH_TYPE` and `OP_AUTH_STATUS`.
- Motus active insurance does not contain `cancel_effective_date`; current membership in the Motus Insur dataset indicates an active or pending policy.
- Future-effective cancellations are present in Motus InsHist and must be queried there if the existing warning is retained.
- Motus authority history is a status-change event stream (`OP_AUTH_STATUS`, `REASON`, `STATUS_CHANGE_DATE`), not the legacy original/final action layout.
- Motus revoke/suspend contains suspensions as well as revocations.
- Docket values are not consistently formatted across datasets. Carrier/AuthHist examples use `MC1426065`, while RevokeSuspend examples use `MC-1426065`.

For MVP, store only identifiers, authority/bond decision fields, dates, response/debug identity fields, source lineage, and the full source row in `raw_record JSONB`. Do not reproduce every Motus field as a normal column.

Recommended tables:

- `fmcsa_motus_carriers`
- `fmcsa_motus_active_insurance`
- `fmcsa_motus_insurance_history`
- `fmcsa_motus_revocations`
- `fmcsa_motus_authority_history`
- `fmcsa_import_jobs`

The current broker-check decision can be implemented from carrier, active insurance, and insurance history. Revocation and authority history are useful for audit/debugging and future decision reasons, but the current service does not query them.

## Scope and evidence

### Repository state

The requested branch names were not present locally or on the configured GitHub remotes at discovery time:

- Requested importer branch: `codex/motus-native-tables`
- Checked-out importer branch: `codex/motus-ingestion-adapter`
- Requested broker-check branch: `codex/motus-native-broker-check`
- Checked-out broker-check branch: `main`

The importer worktree already contained unrelated modified files. The broker-check worktree already contained untracked `outputs/` and `scripts/`. No branch switch was performed and none of those changes were modified.

This document uses:

- The live PostgreSQL `information_schema` and `pg_indexes` for the five existing legacy tables.
- Importer migration `migrations/001_create_fmcsa_source_tables.sql`.
- Broker-check code on the checked-out `main` branch.
- Official DOT/Socrata metadata and CSV samples as of 2026-06-21.
- The official `USDOT Motus Operating Authority Data Dictionary`, dated 2026-05-18.
- The official legacy `FMCSA Dataset Description and Data Definitions - Select Datasets`.

### Important dataset-ID finding

The current importer config has correct Motus daily-difference IDs, but its `MOTUS_DATASETS.allHist` entries point to legacy all-history assets.

| Dataset | Current configured all-history ID | What it actually is | Correct Motus all-history ID |
|---|---:|---|---:|
| carrier | `u4i8-4m26` | Legacy Carrier - All With History, headerless legacy row layout | `inys-ebih` |
| active insurance | `y77m-3nfx` | Legacy ActPendInsur - All With History, headerless legacy row layout | `c5y8-a4uz` |
| insurance history | `nzpz-e5xn` | Legacy InsHist - All With History, headerless legacy row layout | `3uet-3z4i` |
| revocation | `rwr4-5nkg` | Legacy Revocation - All With History, headerless legacy row layout | `wb4f-neki` |
| authority history | `wahn-z3rq` | Legacy AuthHist - All With History, headerless legacy row layout | `yu5v-wbh6` |

The correct Motus all-history datasets are normal Socrata CSV datasets with headers. They have the same columns as their corresponding Motus daily-difference datasets.

## Existing legacy database schemas

The schemas below were verified against the live PostgreSQL database. All tables use a `BIGSERIAL`/`bigint` primary key and have a unique B-tree index on `source_record_hash`.

“Broker-check use” includes lookup, decision logic, response fields, warnings, and ordering.

### `fmcsa_carriers`

| Column | PostgreSQL type | Null | Broker-check use |
|---|---|---:|---|
| `id` | `bigint` | no | Tie-breaker in lookup ordering |
| `docket_number` | `text` | no | MC lookup and response |
| `dot_number` | `text` | no | DOT fallback lookup and response |
| `legal_name` | `text` | yes | Company-name mismatch warning and response |
| `dba_name` | `text` | yes | Response |
| `broker_stat` | `text` | yes | Broker authority ACTIVE decision |
| `common_stat` | `text` | yes | Not used |
| `contract_stat` | `text` | yes | Not used |
| `broker_app_pend` | `text` | yes | Not used |
| `broker_rev_pend` | `text` | yes | Not used |
| `bond_req` | `text` | yes | Not used |
| `bond_file` | `text` | yes | Bond/insurance-on-file YES decision |
| `bipd_file` | `text` | yes | Not used |
| `cargo_file` | `text` | yes | Not used |
| `bus_street_po` | `text` | yes | Response |
| `bus_city` | `text` | yes | City mismatch warning and response |
| `bus_state_code` | `text` | yes | State mismatch warning and response |
| `bus_ctry_code` | `text` | yes | Response |
| `bus_zip_code` | `text` | yes | Response |
| `bus_telno` | `text` | yes | Response |
| `mail_street_po` | `text` | yes | Response |
| `mail_city` | `text` | yes | Response |
| `mail_state_code` | `text` | yes | Response |
| `mail_ctry_code` | `text` | yes | Response |
| `mail_zip_code` | `text` | yes | Response |
| `source_record_hash` | `text` | no | Import deduplication only |
| `raw_record` | `jsonb` | yes | Not used by broker-check |
| `imported_at` | `timestamptz` | no | Not used |
| `updated_at` | `timestamptz` | no | Select latest matching carrier row |

Indexes and constraints:

- Primary key: `fmcsa_carriers_pkey (id)`
- Unique: `idx_fmcsa_carriers_source_record_hash (source_record_hash)`
- B-tree: `idx_fmcsa_carriers_docket_number (docket_number)`
- B-tree: `idx_fmcsa_carriers_dot_number (dot_number)`

### `fmcsa_active_pending_insurance`

| Column | PostgreSQL type | Null | Broker-check use |
|---|---|---:|---|
| `id` | `bigint` | no | Tie-breaker |
| `docket_number` | `text` | no | MC bond lookup |
| `dot_number` | `text` | no | DOT fallback bond lookup |
| `ins_form_code` | `text` | yes | Restrict to BMC-84/BMC-85 |
| `insurance_type_description` | `text` | yes | Not used |
| `insurance_company_name` | `text` | yes | Response |
| `policy_no` | `text` | yes | Loaded by ORM but not returned/used in the decision |
| `posted_date` | `date` | yes | Not used |
| `effective_date` | `date` | yes | Must be on or before check date; latest ordering |
| `cancel_effective_date` | `date` | yes | Active filter and future-cancellation warning |
| `source_record_hash` | `text` | no | Import deduplication only |
| `raw_record` | `jsonb` | yes | Not used by broker-check |
| `imported_at` | `timestamptz` | no | Not used |
| `updated_at` | `timestamptz` | no | Not used |

Indexes and constraints:

- Primary key: `fmcsa_active_pending_insurance_pkey (id)`
- Unique: `idx_fmcsa_active_pending_insurance_source_record_hash (source_record_hash)`
- B-tree: `idx_fmcsa_active_pending_insurance_docket_number (docket_number)`
- B-tree: `idx_fmcsa_active_pending_insurance_dot_number (dot_number)`

### `fmcsa_insurance_history`

| Column | PostgreSQL type | Null | Broker-check use |
|---|---|---:|---|
| `id` | `bigint` | no | Tie-breaker |
| `docket_number` | `text` | no | MC history lookup |
| `dot_number` | `text` | no | DOT fallback history lookup |
| `ins_form_code` | `text` | yes | Restrict to BMC-84/BMC-85 and response |
| `cancellation_method` | `text` | yes | Response context only; never fails the check |
| `insurance_type_description` | `text` | yes | Not used |
| `policy_no` | `text` | yes | Not used by current code |
| `effective_date` | `date` | yes | Latest-history ordering and response |
| `cancel_effective_date` | `date` | yes | Response |
| `specific_cancellation_method` | `text` | yes | Not used |
| `insurance_company_name` | `text` | yes | Loaded by ORM but not returned/used |
| `source_record_hash` | `text` | no | Import deduplication only |
| `raw_record` | `jsonb` | yes | Not used by broker-check |
| `imported_at` | `timestamptz` | no | Not used |
| `updated_at` | `timestamptz` | no | Not used |

Indexes and constraints:

- Primary key: `fmcsa_insurance_history_pkey (id)`
- Unique: `idx_fmcsa_insurance_history_source_record_hash (source_record_hash)`
- B-tree: `idx_fmcsa_insurance_history_docket_number (docket_number)`
- B-tree: `idx_fmcsa_insurance_history_dot_number (dot_number)`

### `fmcsa_revocations`

| Column | PostgreSQL type | Null | Broker-check use |
|---|---|---:|---|
| `id` | `bigint` | no | Not used |
| `docket_number` | `text` | no | Not used |
| `dot_number` | `text` | no | Not used |
| `authority_type` | `text` | yes | Not used |
| `serve_date` | `date` | yes | Not used |
| `revocation_type` | `text` | yes | Not used |
| `effective_date` | `date` | yes | Not used |
| `source_record_hash` | `text` | no | Import deduplication only |
| `raw_record` | `jsonb` | yes | Not used |
| `imported_at` | `timestamptz` | no | Not used |
| `updated_at` | `timestamptz` | no | Not used |

Indexes and constraints:

- Primary key: `fmcsa_revocations_pkey (id)`
- Unique: `idx_fmcsa_revocations_source_record_hash (source_record_hash)`
- B-tree: `idx_fmcsa_revocations_docket_number (docket_number)`
- B-tree: `idx_fmcsa_revocations_dot_number (dot_number)`

### `fmcsa_authority_history`

| Column | PostgreSQL type | Null | Broker-check use |
|---|---|---:|---|
| `id` | `bigint` | no | Not used |
| `docket_number` | `text` | no | Not used |
| `dot_number` | `text` | no | Not used |
| `sub_number` | `text` | yes | Not used |
| `authority_type` | `text` | yes | Not used |
| `original_action` | `text` | yes | Not used |
| `original_action_date` | `date` | yes | Not used |
| `final_action` | `text` | yes | Not used |
| `final_decision_date` | `date` | yes | Not used |
| `final_served_date` | `date` | yes | Not used |
| `is_broker_authority` | `boolean` | no | Not used |
| `is_carrier_authority` | `boolean` | no | Not used |
| `is_negative_final_action` | `boolean` | no | Not used |
| `is_revoked` | `boolean` | no | Not used |
| `is_reinstated` | `boolean` | no | Not used |
| `is_discontinued_revocation` | `boolean` | no | Not used |
| `source_record_hash` | `text` | no | Import deduplication only |
| `raw_record` | `jsonb` | yes | Not used |
| `imported_at` | `timestamptz` | no | Not used |
| `updated_at` | `timestamptz` | no | Not used |

Indexes and constraints:

- Primary key: `fmcsa_authority_history_pkey (id)`
- Unique: `idx_fmcsa_authority_history_source_record_hash (source_record_hash)`
- B-tree: `idx_fmcsa_authority_history_docket_number (docket_number)`
- B-tree: `idx_fmcsa_authority_history_dot_number (dot_number)`
- B-tree: `idx_fmcsa_authority_history_is_broker_authority (is_broker_authority)`
- B-tree: `idx_fmcsa_authority_history_dot_docket_broker (dot_number, docket_number, is_broker_authority)`

## Exact broker-check field and decision usage

Code references are in `/Users/jakhongirmasaidov/Projects/IT_projects/Dispatch_AI/broker-check-simplified`.

### Company identity lookup

- Input MC is normalized to `MC` plus at least six digits.
- Input DOT is normalized to eight digits.
- Lookup is MC/docket first, then DOT.
- The selected carrier is ordered by `updated_at DESC`, then `id DESC`.
- Company name is not used to locate the record. It only produces a warning when input `companyName` differs from `legal_name`.
- City and state are not used to locate the record. They only produce a warning when both sides are populated and differ.

Code:

- `app/utils.py:9-24`
- `app/repository.py:11-29`
- `app/service.py:32-52`
- `app/service.py:205-219`

### MC / DOT matching

- Carrier, active bond, and insurance-history lookups all try normalized MC/docket first.
- DOT is only a fallback when no MC match is found.
- Matching is exact string equality.
- No cross-check rejects a record when MC matches but the supplied DOT points to a different entity.

Motus implication: ingest and query a canonical docket key because Motus source datasets use both hyphenated and unhyphenated values. Retain the original source value in `raw_record`.

### Broker authority status

- Current decision source: `fmcsa_carriers.broker_stat`.
- Accepted active values: `A` or `ACTIVE`, case-insensitive after trim.
- Any other value fails with `Broker authority is not ACTIVE.`

Motus replacement:

- Filter/select the OA row whose `OP_AUTH_TYPE` is a broker authority, currently observed as `Broker of Property (Except Household Goods)`.
- Evaluate `OP_AUTH_STATUS`.
- Observed broker statuses in the all-history dataset: `Active`, `Inactive`, `Pending`, and `Withdrawn`.

### Bond status

The service applies two checks:

1. Carrier-level `bond_file` must be YES-like (`Y`, `YES`, `TRUE`, or `1`).
2. A current BMC-84/BMC-85 row must exist in active/pending insurance.

The first failure reason is `Bond / Insurance on File is not YES.`

Motus replacement:

- Carrier `BOND_FILE` is directly available.
- Active insurance `INS_FORM_CODE` uses values such as `BMC-84` and `BMC-85`.
- `INS_TYPE_CODE` values `3` and `4` identify bond and trust fund respectively and should be retained as a second validation signal.

### Active insurance status

Current legacy query requires:

- Matching MC or fallback DOT.
- BMC-84/BMC-85 form.
- `effective_date <= today`.
- `cancel_effective_date IS NULL OR cancel_effective_date > today`.

Motus difference:

- Motus Insur contains active or pending policies and has no cancellation-effective-date column.
- Therefore, current-state dataset membership plus `effective_date <= today` is the active-policy test.
- The daily-difference feed may represent removal as a blank/tombstone row where only docket is populated. The importer must apply the tombstone to the current-state table; it must not insert that row as an active policy.

### Insurance cancellation logic

Current behavior:

- A future `cancel_effective_date` on the selected active bond does not fail.
- It adds `Broker bond/trust has future cancellation date: YYYY-MM-DD.`
- Historical cancellation method and cancellation date are returned only as context.
- Old cancellation records do not fail a broker when a current active bond exists.

Motus replacement:

- Motus active insurance has no cancellation date.
- Query Motus InsHist for a matching current bond/trust policy using canonical docket, form, policy number, and preferably effective date.
- If a matching history row has `CANCL_EFFECTIVE_DATE > today`, preserve the warning.
- Do not fail based only on an old history row.
- Exact duplicate and near-duplicate InsHist rows exist in official samples, so select/deduplicate deterministically.

Confidence in the future-cancellation join is **medium**. The fields support it and future-effective BMC-84 cancellations exist in the official all-history data, but matching behavior should be validated against representative production entities before becoming a blocker.

### Revocation / suspension logic

There is none in the current broker-check service.

- No revocation model is defined.
- No revocation or authority-history repository query exists.
- The README explicitly says revocation history is not validated.

For MVP parity, do not independently fail from historical revoke/suspend rows. Current `OP_AUTH_STATUS != Active` already fails the broker.

If explicit suspension/revocation reasons are added later:

- Restrict to broker `OP_AUTH_TYPE`.
- Consider only actions effective on or before the check date.
- Reconcile them with later AuthHist events such as `REINSTATED` or `Discontinued Revocation`.
- Treat current carrier `OP_AUTH_STATUS` as the primary current-state signal, not a historical action in isolation.

### PASS/FAIL reasons

| Outcome | Exact current reason | Fields involved |
|---|---|---|
| FAIL | `Carrier record not found by MC or DOT number.` | `docket_number`, `dot_number` |
| FAIL | `Broker authority is not ACTIVE.` | carrier `broker_stat`; Motus `OP_AUTH_TYPE`, `OP_AUTH_STATUS` |
| FAIL | `Bond / Insurance on File is not YES.` | carrier `bond_file`; Motus `BOND_FILE` |
| FAIL | `No active BMC-84/BMC-85 bond/trust exists in active_pending_insurance.` | active insurance identifiers, form, effective/cancel dates |
| PASS | `Broker authority is ACTIVE and active BMC-84/BMC-85 bond/trust is on file.` | authority status plus carrier bond flag plus active policy |

Warnings:

- Company name differs: `legal_name`.
- City/state differs: `bus_city`, `bus_state_code`.
- Future bond cancellation: legacy active `cancel_effective_date`; Motus history `CANCL_EFFECTIVE_DATE`.

Response-only fields:

- Carrier: DBA, full business address, phone, full mailing address.
- Active insurance: form, effective date, cancellation date, company name.
- History: latest form, effective date, cancellation date, cancellation reason.

Legacy `undeliverableMail` and business fax are currently hard-coded to `null` by model properties. Motus provides separate business and mailing undeliverable-mail fields, but no business fax.

## Official Motus datasets and lightweight file validation

All daily-difference and correct Motus all-history datasets were inspected through official Socrata metadata and bounded CSV queries (`$limit=3` or targeted filtered samples). No full Carrier all-history file was downloaded or imported.

| Logical dataset | Daily difference | Correct all-history | Format/header result |
|---|---:|---:|---|
| carrier | `nakq-58th` | `inys-ebih` | CSV, header present, 28 columns |
| active insurance | `x96h-evps` (Motus Insur) | `c5y8-a4uz` | CSV, header present, 11 columns |
| insurance history | `xe5s-wca7` (Motus InsHist) | `3uet-3z4i` | CSV, header present, 15 columns |
| revocation/suspension | `e67p-xyd5` | `wb4f-neki` | CSV, header present, 6 columns |
| authority history | `dm5j-zc6c` | `yu5v-wbh6` | CSV, header present, 6 columns |

Observed source date format is compact `YYYYMMDD` text. Convert valid values to PostgreSQL `date` and preserve the original text in `raw_record`.

## Dataset comparisons and storage recommendations

### Carrier

Confidence: **high** for field availability and MVP usage; **medium** for an enforceable natural uniqueness key because FMCSA explicitly permits duplicates.

#### Legacy columns

The legacy table flattens all authority categories into `broker_stat`, `common_stat`, `contract_stat`, pending/revocation flags, and shared company/address fields.

#### Motus available fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `RFC_NUMBER`, `OP_AUTH_TYPE`, `OP_AUTH_STATUS`, `MIN_COV_AMOUNT`, `CARGO_REQ`, `BOND_REQ`, `BIPD_FILE`, `CARGO_FILE`, `BOND_FILE`, `BUS_UNDELIVERABLE_MAIL`, `MAIL_UNDELIVERABLE_MAIL`, `DBA_NAME`, `LEGAL_NAME`, `BUS_STREET_PO`, `BUS_COLONIA`, `BUS_CITY`, `BUS_STATE_CODE`, `BUS_CTRY_CODE`, `BUS_ZIP_CODE`, `BUS_TELNO`, `MAIL_STREET_PO`, `MAIL_COLONIA`, `MAIL_CITY`, `MAIL_STATE_CODE`, `MAIL_CTRY_CODE`, `MAIL_ZIP_CODE`.

Motus removes legacy `BROKER_STAT`, `COMMON_STAT`, `CONTRACT_STAT`, authority pending/revocation columns, authority category checkboxes, business fax, mailing phone, and mailing fax.

#### Broker-check required fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `OP_AUTH_TYPE`, `OP_AUTH_STATUS`, `BOND_REQ`, `BOND_FILE`, `LEGAL_NAME`, `DBA_NAME`, business street/city/state/country/zip/phone, mailing street/city/state/country/zip, and optionally both undeliverable-mail fields.

#### Normal columns to store

- Identifiers and authority: `docket_number`, `docket_number_canonical`, `usdot_number`, `op_auth_type`, `op_auth_status`, `is_broker_authority`.
- Bond decision: `bond_req`, `bond_file`.
- Identity/warnings/response: `legal_name`, `dba_name`.
- Business response: `bus_street_po`, `bus_city`, `bus_state_code`, `bus_ctry_code`, `bus_zip_code`, `bus_telno`, `bus_undeliverable_mail`.
- Mailing response: `mail_street_po`, `mail_city`, `mail_state_code`, `mail_ctry_code`, `mail_zip_code`, `mail_undeliverable_mail`.
- Lineage: `source_record_hash`, `raw_record`, `import_job_id`, `imported_at`, `updated_at`.

#### Keep only in `raw_record`

`RFC_NUMBER`, `MIN_COV_AMOUNT`, `CARGO_REQ`, `BIPD_FILE`, `CARGO_FILE`, `BUS_COLONIA`, and `MAIL_COLONIA`.

These fields are not used for broker lookup, status, bond checks, current response output, or mismatch warnings. Promote one later only when a concrete product/query requirement exists.

### Active insurance

Confidence: **high** for available fields and active-policy logic; **medium** for current-state deletion/tombstone behavior until importer E2E tests cover it.

#### Legacy columns

`docket_number`, `dot_number`, `ins_form_code`, `insurance_type_description`, `insurance_company_name`, `policy_no`, `posted_date`, `effective_date`, `cancel_effective_date`.

#### Motus available fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `INS_FORM_CODE`, `INS_TYPE_CODE`, `INS_CLASS_CODE`, `MAX_COV_AMOUNT`, `UNDERL_LIM_AMOUNT`, `POLICY_NO`, `EFFECTIVE_DATE`, `INSURANCE_COMPANY_NAME`, `TRANS_DATE`.

Motus has no active-insurance cancellation date and no direct legacy insurance-type-description field.

#### Broker-check required fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `INS_FORM_CODE`, `INS_TYPE_CODE`, `POLICY_NO`, `EFFECTIVE_DATE`, `INSURANCE_COMPANY_NAME`, and `TRANS_DATE` for deterministic ordering/debugging.

#### Normal columns to store

- `docket_number`, `docket_number_canonical`, `usdot_number`
- `ins_form_code`, `ins_type_code`
- `policy_no`
- `effective_date`
- `insurance_company_name`
- `trans_date`
- `source_record_hash`, `raw_record`, `import_job_id`, `imported_at`, `updated_at`

#### Keep only in `raw_record`

`INS_CLASS_CODE`, `MAX_COV_AMOUNT`, and `UNDERL_LIM_AMOUNT`.

They describe BI&PD coverage and are not needed to validate BMC-84/BMC-85 existence. They may be promoted if coverage-amount validation becomes a product requirement.

#### Special ingestion requirement

Motus documentation says daily Insur removals may be blank records other than docket number. A Motus-native current-state table needs explicit delete/tombstone handling. A blank row must never satisfy the active-bond query.

### Insurance history

Confidence: **high** for field availability; **medium** for the future-cancellation policy join due to duplicates and source semantics.

#### Legacy columns

`docket_number`, `dot_number`, `ins_form_code`, `cancellation_method`, `insurance_type_description`, `policy_no`, `effective_date`, `cancel_effective_date`, `specific_cancellation_method`, `insurance_company_name`.

#### Motus available fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `INS_FORM_CODE`, `FILING_STATUS_REASON`, `INS_TYPE_CODE`, `INS_TYPE_IND`, `POLICY_NO`, `INS_TYPE_DESC`, `MIN_COV_AMOUNT`, `INS_CLASS_CODE`, `EFFECTIVE_DATE`, `UNDERL_LIM_AMOUNT`, `MAX_COV_AMOUNT`, `CANCL_EFFECTIVE_DATE`, `INSURANCE_COMPANY_NAME`.

#### Broker-check required fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `INS_FORM_CODE`, `FILING_STATUS_REASON`, `INS_TYPE_CODE`, `POLICY_NO`, `EFFECTIVE_DATE`, `CANCL_EFFECTIVE_DATE`, `INSURANCE_COMPANY_NAME`, and optionally `INS_TYPE_DESC` for response/debugging.

#### Normal columns to store

- `docket_number`, `docket_number_canonical`, `usdot_number`
- `ins_form_code`, `filing_status_reason`, `ins_type_code`, `ins_type_desc`
- `policy_no`
- `effective_date`, `cancel_effective_date`
- `insurance_company_name`
- `source_record_hash`, `raw_record`, `import_job_id`, `imported_at`, `updated_at`

#### Keep only in `raw_record`

`INS_TYPE_IND`, `MIN_COV_AMOUNT`, `INS_CLASS_CODE`, `UNDERL_LIM_AMOUNT`, and `MAX_COV_AMOUNT`.

Observed all-history samples contain exact duplicates and near-duplicates where coverage amounts differ while policy identity fields are the same. Therefore, do not enforce a broad composite unique constraint as the sole history identity.

### Revocation / revoke-suspend

Confidence: **high** for schema; **medium** for future decision semantics because current broker-check does not use it and history must be reconciled with reinstatement.

#### Legacy columns

`docket_number`, `dot_number`, `authority_type`, `serve_date`, `revocation_type`, `effective_date`.

#### Motus available fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `OP_AUTH_TYPE`, `ORDER1_SERVE_DATE`, `ORDER1_TYPE_DESC`, `ORDER1_EFFECTIVE_DATE`.

All six fields map directly, but Motus expands the meaning from revocation-only to revoke/suspend actions.

#### Broker-check required fields

None for current parity. If explicit revocation/suspension reasons are added, all six fields are required.

#### Normal columns to store

- `docket_number`, `docket_number_canonical`, `usdot_number`
- `op_auth_type`, `is_broker_authority`
- `serve_date`, `action_type_description`, `effective_date`
- `source_record_hash`, `raw_record`, `import_job_id`, `imported_at`, `updated_at`

#### Keep only in `raw_record`

No source field needs to be JSON-only; this dataset has only six useful fields. The full row must still be retained in `raw_record`.

### Authority history

Confidence: **high** for schema and current-status event representation; **medium** for any derived “unresolved revocation” flag.

#### Legacy columns

`docket_number`, `dot_number`, `sub_number`, `authority_type`, `original_action`, `original_action_date`, `final_action`, `final_decision_date`, `final_served_date`, plus six importer-derived booleans.

#### Motus available fields

`DOCKET_NUMBER`, `USDOT_NUMBER`, `OP_AUTH_TYPE`, `OP_AUTH_STATUS`, `REASON`, `STATUS_CHANGE_DATE`.

Motus removes `SUB_NUMBER` and the original/final action/date layout. Each row is a status-change event.

#### Broker-check required fields

None for current parity. For audit and future explicit status reasons: all six fields.

#### Normal columns to store

- `docket_number`, `docket_number_canonical`, `usdot_number`
- `op_auth_type`, `is_broker_authority`
- `op_auth_status`, `reason`, `status_change_date`
- `source_record_hash`, `raw_record`, `import_job_id`, `imported_at`, `updated_at`

#### Keep only in `raw_record`

No source field needs to be JSON-only; all six are compact and operationally useful.

Do not carry forward the legacy original/final-action columns or the legacy derived flags unchanged. If derived flags are later required, derive them from Motus `OP_AUTH_STATUS` and `REASON` with documented mappings and tests.

## Proposed MVP tables

These are design recommendations, not migration SQL.

Common conventions:

- Store identifiers as `text`, never numeric types; leading zeros and prefixes are meaningful.
- Store source dates as PostgreSQL `date`.
- Store monetary/coverage values only if promoted later; use `numeric`, not floating point.
- Store every original source row in non-null `raw_record jsonb`.
- `source_record_hash` should hash a canonical representation of the complete original row plus dataset identity.
- Use an import-job foreign key for lineage.
- Keep both source `docket_number` and a canonical comparison key. Recommended canonical form: uppercase prefix plus digits with punctuation removed, for example both `MC-1426065` and `MC1426065` become `MC1426065`.
- Do not left-pad Motus USDOT values on ingestion. Store source/canonical digits and normalize request-side values consistently. A display-normalized eight-digit DOT may be computed in the application.

### `fmcsa_motus_carriers`

| Column | Type | Why / broker-check use |
|---|---|---|
| `id` | `bigserial` | Surrogate primary key |
| `docket_number` | `text not null` | Original normalized source identifier for response/debugging |
| `docket_number_canonical` | `text not null` | Hyphen-insensitive MC/FF/MX lookup |
| `usdot_number` | `text not null` | DOT fallback lookup |
| `op_auth_type` | `text not null` | Select broker OA rather than an arbitrary carrier OA |
| `op_auth_status` | `text` | ACTIVE/FAIL decision |
| `is_broker_authority` | `boolean not null` | Stable, tested classification for query performance |
| `bond_req` | `text` | Decision/debug context |
| `bond_file` | `text` | YES/NO decision |
| `legal_name` | `text` | Company identity warning and response |
| `dba_name` | `text` | Response |
| `bus_street_po` | `text` | Response |
| `bus_city` | `text` | City mismatch warning and response |
| `bus_state_code` | `text` | State mismatch warning and response |
| `bus_ctry_code` | `text` | Response |
| `bus_zip_code` | `text` | Response |
| `bus_telno` | `text` | Response |
| `bus_undeliverable_mail` | `text` | Replaces currently-null response field with source data |
| `mail_street_po` | `text` | Response |
| `mail_city` | `text` | Response |
| `mail_state_code` | `text` | Response |
| `mail_ctry_code` | `text` | Response |
| `mail_zip_code` | `text` | Response |
| `mail_undeliverable_mail` | `text` | Debug/response context |
| `source_record_hash` | `text not null` | Source-row idempotency |
| `raw_record` | `jsonb not null` | Full original row |
| `import_job_id` | `bigint not null` | Import lineage |
| `imported_at` | `timestamptz not null` | Audit |
| `updated_at` | `timestamptz not null` | Current-row ordering/debug |

Suggested indexes:

- Primary key on `id`.
- Unique on `source_record_hash`.
- B-tree on `(docket_number_canonical, is_broker_authority, updated_at desc)`.
- B-tree on `(usdot_number, is_broker_authority, updated_at desc)`.
- Optional partial B-tree on `(docket_number_canonical, op_auth_status)` where `is_broker_authority`.

Suggested business key:

- Logical lookup key: `(docket_number_canonical, usdot_number, op_auth_type)`.
- Do not initially enforce it as unique because official metadata says docket and USDOT values can duplicate and legacy data may contain multiple OAs per docket.
- Enforced idempotency key: `source_record_hash`.

### `fmcsa_motus_active_insurance`

| Column | Type | Why / broker-check use |
|---|---|---|
| `id` | `bigserial` | Surrogate primary key |
| `docket_number` | `text not null` | Source identifier |
| `docket_number_canonical` | `text not null` | MC lookup |
| `usdot_number` | `text not null` | DOT fallback |
| `ins_form_code` | `text` | BMC-84/BMC-85 filter |
| `ins_type_code` | `text` | Bond/trust validation (`3`/`4`) |
| `policy_no` | `text` | Policy identity and future-cancellation join |
| `effective_date` | `date` | Current-date eligibility and latest selection |
| `insurance_company_name` | `text` | Response |
| `trans_date` | `date` | Receipt ordering/debug |
| `source_record_hash` | `text not null` | Idempotency/deduplication |
| `raw_record` | `jsonb not null` | Full original row |
| `import_job_id` | `bigint not null` | Import lineage |
| `imported_at` | `timestamptz not null` | Audit |
| `updated_at` | `timestamptz not null` | Audit/current-state maintenance |

Suggested indexes:

- Primary key on `id`.
- Unique on `source_record_hash`.
- B-tree on `(docket_number_canonical, ins_form_code, effective_date desc)`.
- B-tree on `(usdot_number, ins_form_code, effective_date desc)`.
- Partial indexes for BMC-84/BMC-85 are appropriate after source-value normalization is fixed.
- B-tree on `(docket_number_canonical, policy_no, ins_form_code)` for history matching.

Suggested business key:

- Logical policy key: `(docket_number_canonical, ins_form_code, policy_no, effective_date, insurance_company_name)`.
- Use `source_record_hash` as the initial enforced unique key because exact duplicate source rows exist and blank removal records require importer-specific handling.

### `fmcsa_motus_insurance_history`

| Column | Type | Why / broker-check use |
|---|---|---|
| `id` | `bigserial` | Surrogate primary key |
| `docket_number` | `text not null` | Source identifier |
| `docket_number_canonical` | `text not null` | MC lookup |
| `usdot_number` | `text not null` | DOT fallback |
| `ins_form_code` | `text` | BMC-84/BMC-85 filter |
| `filing_status_reason` | `text` | Cancellation/replacement response context |
| `ins_type_code` | `text` | Bond/trust validation |
| `ins_type_desc` | `text` | Debug/response context |
| `policy_no` | `text` | Match current policy to cancellation history |
| `effective_date` | `date` | Latest-history ordering and policy match |
| `cancel_effective_date` | `date` | Future-cancellation warning and response |
| `insurance_company_name` | `text` | Debug/policy match |
| `source_record_hash` | `text not null` | Idempotency/deduplication |
| `raw_record` | `jsonb not null` | Full original row |
| `import_job_id` | `bigint not null` | Import lineage |
| `imported_at` | `timestamptz not null` | Audit |
| `updated_at` | `timestamptz not null` | Audit |

Suggested indexes:

- Primary key on `id`.
- Unique on `source_record_hash`.
- B-tree on `(docket_number_canonical, ins_form_code, effective_date desc)`.
- B-tree on `(usdot_number, ins_form_code, effective_date desc)`.
- B-tree on `(docket_number_canonical, policy_no, ins_form_code, cancel_effective_date)`.

Suggested business key:

- Logical event key: `(docket_number_canonical, ins_form_code, policy_no, effective_date, cancel_effective_date, filing_status_reason)`.
- Do not enforce this composite initially. Official data contains duplicates and near-duplicates. Enforce only `source_record_hash` until profiling establishes safe event identity.

### `fmcsa_motus_revocations`

| Column | Type | Why / broker-check use |
|---|---|---|
| `id` | `bigserial` | Surrogate primary key |
| `docket_number` | `text not null` | Source identifier |
| `docket_number_canonical` | `text not null` | Cross-dataset MC lookup |
| `usdot_number` | `text not null` | DOT lookup |
| `op_auth_type` | `text` | Restrict to broker OA |
| `is_broker_authority` | `boolean not null` | Stable query classification |
| `serve_date` | `date` | Action timeline |
| `action_type_description` | `text` | Suspension/revocation reason |
| `effective_date` | `date` | Determine whether action is effective as of check date |
| `source_record_hash` | `text not null` | Idempotency |
| `raw_record` | `jsonb not null` | Full original row |
| `import_job_id` | `bigint not null` | Import lineage |
| `imported_at` | `timestamptz not null` | Audit |
| `updated_at` | `timestamptz not null` | Audit |

Suggested indexes:

- Primary key on `id`.
- Unique on `source_record_hash`.
- B-tree on `(docket_number_canonical, is_broker_authority, effective_date desc)`.
- B-tree on `(usdot_number, is_broker_authority, effective_date desc)`.

Suggested business key:

- Logical action key: `(docket_number_canonical, op_auth_type, serve_date, action_type_description, effective_date)`.
- Enforce `source_record_hash` initially; FMCSA permits repeated actions per OA.

### `fmcsa_motus_authority_history`

| Column | Type | Why / broker-check use |
|---|---|---|
| `id` | `bigserial` | Surrogate primary key |
| `docket_number` | `text not null` | Source identifier |
| `docket_number_canonical` | `text not null` | Cross-dataset MC lookup |
| `usdot_number` | `text not null` | DOT lookup |
| `op_auth_type` | `text` | Restrict to broker OA |
| `is_broker_authority` | `boolean not null` | Stable query classification |
| `op_auth_status` | `text` | Historical status |
| `reason` | `text` | Explain status change/reinstatement |
| `status_change_date` | `date` | Event ordering |
| `source_record_hash` | `text not null` | Idempotency |
| `raw_record` | `jsonb not null` | Full original row |
| `import_job_id` | `bigint not null` | Import lineage |
| `imported_at` | `timestamptz not null` | Audit |
| `updated_at` | `timestamptz not null` | Audit |

Suggested indexes:

- Primary key on `id`.
- Unique on `source_record_hash`.
- B-tree on `(docket_number_canonical, is_broker_authority, status_change_date desc, id desc)`.
- B-tree on `(usdot_number, is_broker_authority, status_change_date desc, id desc)`.

Suggested business key:

- Logical event key: `(docket_number_canonical, op_auth_type, op_auth_status, reason, status_change_date)`.
- Enforce `source_record_hash` initially because multiple authority changes per OA are expected.

### `fmcsa_import_jobs`

This table should replace/extend the role of `fmcsa_raw_imports` for explicit job state and per-provider/dataset lineage.

| Column | Type | Why |
|---|---|---|
| `id` | `bigserial` | Primary key referenced by Motus rows |
| `provider` | `text not null` | `motus` or `legacy` |
| `dataset_name` | `text not null` | Stable logical dataset name |
| `dataset_id` | `text not null` | Socrata resource ID |
| `source_kind` | `text not null` | `daily_diff` or `all_history` |
| `source_url` | `text` | Debug/reproducibility |
| `source_filename` | `text` | Debug/reproducibility |
| `source_etag` | `text` | Source identity |
| `source_last_modified` | `timestamptz` | Source identity/freshness |
| `source_content_length` | `bigint` | Audit |
| `source_sha256` | `text` | Source identity/integrity |
| `status` | `text not null` | `started`, `succeeded`, `failed`, `skipped` |
| `started_at` | `timestamptz not null` | Operations |
| `finished_at` | `timestamptz` | Operations |
| `rows_read` | `bigint` | Audit |
| `rows_inserted` | `bigint` | Audit |
| `rows_updated` | `bigint` | Audit |
| `rows_deleted` | `bigint` | Required for Motus tombstone/current-state handling |
| `rows_failed` | `bigint` | Audit |
| `error_message` | `text` | Failure diagnosis |
| `metadata` | `jsonb not null default '{}'` | Non-query job details |

Suggested indexes and unique key:

- Primary key on `id`.
- B-tree on `(provider, dataset_name, started_at desc)`.
- B-tree on `(status, started_at desc)`.
- Partial unique source identity using provider, dataset ID, source kind, and the best available immutable source identity (`sha256`, otherwise ETag/last-modified).
- Do not use filename alone as uniqueness.

## Required broker-check code changes after tables exist

No code was changed in this discovery task. A later implementation will need:

1. Add Motus SQLAlchemy models and point repositories to Motus-native tables.
2. Canonicalize docket values consistently across request input and all Motus datasets; support `MC`, `FF`, and `MX`, and ignore source hyphens.
3. Stop selecting an arbitrary carrier row. Select the row classified as broker authority.
4. Replace legacy `broker_stat` with Motus `op_auth_status`.
5. Keep carrier `bond_file`, but validate a current BMC-84/BMC-85 policy in Motus active insurance.
6. Remove the active-table cancellation-date predicate because Motus Insur has no cancellation date.
7. Preserve future-cancellation warnings by matching the active policy to Motus insurance history.
8. Map `filing_status_reason` into the existing insurance-history response field, or rename the API field in a versioned response.
9. Populate business/mailing undeliverable-mail response fields from Motus; continue returning fax as null because Motus removed it.
10. Decide whether DOT matching remains fallback-only or whether supplied MC and DOT must be cross-validated. Current behavior can return an MC match even when the input DOT disagrees.
11. Keep revocation/suspension history non-blocking for parity, or introduce separately specified and tested decision rules that reconcile reinstatements.
12. Update tests for Motus values: full authority descriptions, statuses `Active`/`Inactive`/`Pending`/`Withdrawn`, BMC-prefixed form codes, `YYYYMMDD` source dates, duplicate rows, and tombstones.

## Risks and uncertain fields

1. **Wrong all-history IDs in current importer config - high confidence.** The configured IDs resolve to legacy assets; the official catalog exposes separate Motus all-history datasets.
2. **Docket formatting inconsistency - high confidence.** Official Motus samples contain both `MC123456` and `MC-123456` formats across datasets.
3. **No cancellation date in active insurance - high confidence.** Future cancellation logic must move to insurance history.
4. **Daily active-insurance tombstones - high confidence in documented behavior, medium confidence in exact deletion matching.** The official dictionary documents blank removal rows; matching/deletion logic needs sample-driven tests.
5. **Duplicate source records - high confidence.** Exact duplicates were observed in Motus active insurance and insurance history, and near-duplicates exist in history.
6. **Natural keys - medium confidence.** Source semantics identify likely logical keys, but FMCSA explicitly permits repeated docket/USDOT rows. Start with source-row hash uniqueness and profile before enforcing composite business uniqueness.
7. **Broker authority classification - high confidence for the observed property-broker value, medium for exhaustive future values.** Use a tested mapping and retain original `OP_AUTH_TYPE`; do not hard-code one string without monitoring unknown values.
8. **Revocation as a blocker - low confidence without a separate product rule.** A historical action can be superseded by reinstatement. Current carrier status is the safer current-state signal.
9. **DOT normalization - medium confidence.** Official Motus API samples commonly omit legacy eight-digit left padding. Store digits as source/canonical text and normalize both query sides consistently.
10. **Authority-history semantics - high confidence.** Motus rows are status-change events and cannot be safely forced into legacy original/final action columns.

## Final conclusion

Motus-native tables are recommended.

Store:

- Canonical MC/FF/MX and USDOT identifiers.
- Operating-authority type/status and a tested broker-authority classification.
- Carrier bond-required/on-file flags.
- Company identity and address fields currently returned or compared.
- Active bond/trust form, type, policy, effective/transaction dates, and company.
- Insurance-history cancellation reason and dates needed for context and future-cancellation warnings.
- Complete revoke/suspend and authority-history event fields.
- Source hash, import-job lineage, timestamps, and the complete original row in `raw_record JSONB`.

Keep only in `raw_record` for MVP:

- Carrier RFC, coverage amounts, cargo/BIPD flags, and colonia fields.
- Active-insurance class and BI&PD coverage amounts.
- Insurance-history indicator/class and coverage amounts.

Do not copy the legacy flattened authority model or every available Motus column into the new tables.

The broker-check repository must later change its models, authority-row selection, cancellation lookup, identifier normalization, response mapping, and tests. Revocation/suspension should remain non-blocking unless a separate decision specification is approved.

No code, table, migration, broker-check query, scheduler, or AWS deployment change was made as part of this discovery.

## Official references

- [Motus Carrier daily difference](https://data.transportation.gov/d/nakq-58th)
- [Motus Carrier - All With History](https://data.transportation.gov/d/inys-ebih)
- [Motus Insur daily difference](https://data.transportation.gov/d/x96h-evps)
- [Motus Insur - All With History](https://data.transportation.gov/d/c5y8-a4uz)
- [Motus InsHist daily difference](https://data.transportation.gov/d/xe5s-wca7)
- [Motus InsHist - All With History](https://data.transportation.gov/d/3uet-3z4i)
- [Motus RevokeSuspend daily difference](https://data.transportation.gov/d/e67p-xyd5)
- [Motus RevokeSuspend - All With History](https://data.transportation.gov/d/wb4f-neki)
- [Motus AuthHist daily difference](https://data.transportation.gov/d/dm5j-zc6c)
- [Motus AuthHist - All With History](https://data.transportation.gov/d/yu5v-wbh6)
- [DOT Open Data Catalog API](https://api.us.socrata.com/api/catalog/v1)
