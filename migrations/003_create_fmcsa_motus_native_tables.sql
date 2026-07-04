CREATE TABLE IF NOT EXISTS fmcsa_import_jobs (
  id BIGSERIAL PRIMARY KEY,
  provider TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  source_kind TEXT NOT NULL,
  source_url TEXT,
  source_filename TEXT,
  source_etag TEXT,
  source_last_modified TIMESTAMPTZ,
  source_content_length BIGINT,
  source_sha256 TEXT,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  rows_read BIGINT,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  rows_deleted BIGINT,
  rows_failed BIGINT,
  error_message TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_fmcsa_import_jobs_provider_dataset_started
  ON fmcsa_import_jobs (provider, dataset_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_fmcsa_import_jobs_status_started
  ON fmcsa_import_jobs (status, started_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_import_jobs_source_sha256
  ON fmcsa_import_jobs (provider, dataset_id, source_kind, source_sha256)
  WHERE source_sha256 IS NOT NULL;

CREATE TABLE IF NOT EXISTS fmcsa_motus_carriers (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  docket_number_canonical TEXT NOT NULL,
  usdot_number TEXT NOT NULL,
  op_auth_type TEXT NOT NULL,
  op_auth_status TEXT,
  is_broker_authority BOOLEAN NOT NULL,
  bond_req TEXT,
  bond_file TEXT,
  legal_name TEXT,
  dba_name TEXT,
  bus_street_po TEXT,
  bus_city TEXT,
  bus_state_code TEXT,
  bus_ctry_code TEXT,
  bus_zip_code TEXT,
  bus_telno TEXT,
  bus_undeliverable_mail TEXT,
  mail_street_po TEXT,
  mail_city TEXT,
  mail_state_code TEXT,
  mail_ctry_code TEXT,
  mail_zip_code TEXT,
  mail_undeliverable_mail TEXT,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB NOT NULL,
  import_job_id BIGINT NOT NULL REFERENCES fmcsa_import_jobs(id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_motus_carriers_source_hash
  ON fmcsa_motus_carriers (source_record_hash);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_carriers_docket_broker_updated
  ON fmcsa_motus_carriers (docket_number_canonical, is_broker_authority, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_carriers_usdot_broker_updated
  ON fmcsa_motus_carriers (usdot_number, is_broker_authority, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_carriers_broker_status
  ON fmcsa_motus_carriers (docket_number_canonical, op_auth_status)
  WHERE is_broker_authority;

CREATE TABLE IF NOT EXISTS fmcsa_motus_active_insurance (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  docket_number_canonical TEXT NOT NULL,
  usdot_number TEXT NOT NULL,
  ins_form_code TEXT,
  ins_type_code TEXT,
  policy_no TEXT,
  effective_date DATE,
  insurance_company_name TEXT,
  trans_date DATE,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB NOT NULL,
  import_job_id BIGINT NOT NULL REFERENCES fmcsa_import_jobs(id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_motus_active_insurance_source_hash
  ON fmcsa_motus_active_insurance (source_record_hash);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_active_insurance_docket_form_effective
  ON fmcsa_motus_active_insurance (docket_number_canonical, ins_form_code, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_active_insurance_usdot_form_effective
  ON fmcsa_motus_active_insurance (usdot_number, ins_form_code, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_active_insurance_policy_match
  ON fmcsa_motus_active_insurance (docket_number_canonical, policy_no, ins_form_code);

CREATE TABLE IF NOT EXISTS fmcsa_motus_insurance_history (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  docket_number_canonical TEXT NOT NULL,
  usdot_number TEXT NOT NULL,
  ins_form_code TEXT,
  filing_status_reason TEXT,
  ins_type_code TEXT,
  ins_type_desc TEXT,
  policy_no TEXT,
  effective_date DATE,
  cancel_effective_date DATE,
  insurance_company_name TEXT,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB NOT NULL,
  import_job_id BIGINT NOT NULL REFERENCES fmcsa_import_jobs(id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_motus_insurance_history_source_hash
  ON fmcsa_motus_insurance_history (source_record_hash);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_insurance_history_docket_form_effective
  ON fmcsa_motus_insurance_history (docket_number_canonical, ins_form_code, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_insurance_history_usdot_form_effective
  ON fmcsa_motus_insurance_history (usdot_number, ins_form_code, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_insurance_history_policy_match
  ON fmcsa_motus_insurance_history (docket_number_canonical, policy_no, ins_form_code, cancel_effective_date);

CREATE TABLE IF NOT EXISTS fmcsa_motus_revocations (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  docket_number_canonical TEXT NOT NULL,
  usdot_number TEXT NOT NULL,
  op_auth_type TEXT,
  is_broker_authority BOOLEAN NOT NULL,
  serve_date DATE,
  action_type_description TEXT,
  effective_date DATE,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB NOT NULL,
  import_job_id BIGINT NOT NULL REFERENCES fmcsa_import_jobs(id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_motus_revocations_source_hash
  ON fmcsa_motus_revocations (source_record_hash);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_revocations_docket_broker_effective
  ON fmcsa_motus_revocations (docket_number_canonical, is_broker_authority, effective_date DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_revocations_usdot_broker_effective
  ON fmcsa_motus_revocations (usdot_number, is_broker_authority, effective_date DESC);

CREATE TABLE IF NOT EXISTS fmcsa_motus_authority_history (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  docket_number_canonical TEXT NOT NULL,
  usdot_number TEXT NOT NULL,
  op_auth_type TEXT,
  is_broker_authority BOOLEAN NOT NULL,
  op_auth_status TEXT,
  reason TEXT,
  status_change_date DATE,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB NOT NULL,
  import_job_id BIGINT NOT NULL REFERENCES fmcsa_import_jobs(id),
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_motus_authority_history_source_hash
  ON fmcsa_motus_authority_history (source_record_hash);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_authority_history_docket_broker_changed
  ON fmcsa_motus_authority_history (docket_number_canonical, is_broker_authority, status_change_date DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_fmcsa_motus_authority_history_usdot_broker_changed
  ON fmcsa_motus_authority_history (usdot_number, is_broker_authority, status_change_date DESC, id DESC);
