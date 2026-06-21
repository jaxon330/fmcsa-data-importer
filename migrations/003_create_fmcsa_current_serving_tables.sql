CREATE TABLE IF NOT EXISTS fmcsa_current_carriers (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  dot_number TEXT NOT NULL,
  legal_name TEXT,
  dba_name TEXT,
  broker_stat TEXT,
  common_stat TEXT,
  contract_stat TEXT,
  broker_app_pend TEXT,
  broker_rev_pend TEXT,
  bond_req TEXT,
  bond_file TEXT,
  bipd_file TEXT,
  cargo_file TEXT,
  bus_street_po TEXT,
  bus_city TEXT,
  bus_state_code TEXT,
  bus_ctry_code TEXT,
  bus_zip_code TEXT,
  bus_telno TEXT,
  mail_street_po TEXT,
  mail_city TEXT,
  mail_state_code TEXT,
  mail_ctry_code TEXT,
  mail_zip_code TEXT,
  canonical_key TEXT NOT NULL,
  source_provider TEXT NOT NULL,
  source_priority INTEGER NOT NULL,
  last_legacy_seen_at TIMESTAMPTZ,
  last_motus_seen_at TIMESTAMPTZ,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_current_carriers_canonical_key
  ON fmcsa_current_carriers (canonical_key);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_carriers_dot_number
  ON fmcsa_current_carriers (dot_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_carriers_docket_number
  ON fmcsa_current_carriers (docket_number);

CREATE TABLE IF NOT EXISTS fmcsa_current_active_pending_insurance (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  dot_number TEXT NOT NULL,
  ins_form_code TEXT,
  insurance_type_description TEXT,
  insurance_company_name TEXT,
  policy_no TEXT,
  posted_date DATE,
  effective_date DATE,
  cancel_effective_date DATE,
  canonical_key TEXT NOT NULL,
  source_provider TEXT NOT NULL,
  source_priority INTEGER NOT NULL,
  last_legacy_seen_at TIMESTAMPTZ,
  last_motus_seen_at TIMESTAMPTZ,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_current_active_pending_insurance_canonical_key
  ON fmcsa_current_active_pending_insurance (canonical_key);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_active_pending_insurance_dot_number
  ON fmcsa_current_active_pending_insurance (dot_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_active_pending_insurance_docket_number
  ON fmcsa_current_active_pending_insurance (docket_number);

CREATE TABLE IF NOT EXISTS fmcsa_current_insurance_history (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  dot_number TEXT NOT NULL,
  ins_form_code TEXT,
  cancellation_method TEXT,
  insurance_type_description TEXT,
  policy_no TEXT,
  effective_date DATE,
  cancel_effective_date DATE,
  specific_cancellation_method TEXT,
  insurance_company_name TEXT,
  canonical_key TEXT NOT NULL,
  source_provider TEXT NOT NULL,
  source_priority INTEGER NOT NULL,
  last_legacy_seen_at TIMESTAMPTZ,
  last_motus_seen_at TIMESTAMPTZ,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_current_insurance_history_canonical_key
  ON fmcsa_current_insurance_history (canonical_key);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_insurance_history_dot_number
  ON fmcsa_current_insurance_history (dot_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_insurance_history_docket_number
  ON fmcsa_current_insurance_history (docket_number);

CREATE TABLE IF NOT EXISTS fmcsa_current_revocations (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  dot_number TEXT NOT NULL,
  authority_type TEXT,
  serve_date DATE,
  revocation_type TEXT,
  effective_date DATE,
  canonical_key TEXT NOT NULL,
  source_provider TEXT NOT NULL,
  source_priority INTEGER NOT NULL,
  last_legacy_seen_at TIMESTAMPTZ,
  last_motus_seen_at TIMESTAMPTZ,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_current_revocations_canonical_key
  ON fmcsa_current_revocations (canonical_key);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_revocations_dot_number
  ON fmcsa_current_revocations (dot_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_revocations_docket_number
  ON fmcsa_current_revocations (docket_number);

CREATE TABLE IF NOT EXISTS fmcsa_current_authority_history (
  id BIGSERIAL PRIMARY KEY,
  docket_number TEXT NOT NULL,
  dot_number TEXT NOT NULL,
  sub_number TEXT,
  authority_type TEXT,
  original_action TEXT,
  original_action_date DATE,
  final_action TEXT,
  final_decision_date DATE,
  final_served_date DATE,
  is_broker_authority BOOLEAN NOT NULL DEFAULT FALSE,
  is_carrier_authority BOOLEAN NOT NULL DEFAULT FALSE,
  is_negative_final_action BOOLEAN NOT NULL DEFAULT FALSE,
  is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
  is_reinstated BOOLEAN NOT NULL DEFAULT FALSE,
  is_discontinued_revocation BOOLEAN NOT NULL DEFAULT FALSE,
  canonical_key TEXT NOT NULL,
  source_provider TEXT NOT NULL,
  source_priority INTEGER NOT NULL,
  last_legacy_seen_at TIMESTAMPTZ,
  last_motus_seen_at TIMESTAMPTZ,
  source_record_hash TEXT NOT NULL,
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fmcsa_current_authority_history_canonical_key
  ON fmcsa_current_authority_history (canonical_key);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_authority_history_dot_number
  ON fmcsa_current_authority_history (dot_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_authority_history_docket_number
  ON fmcsa_current_authority_history (docket_number);
CREATE INDEX IF NOT EXISTS idx_fmcsa_current_authority_history_is_broker_authority
  ON fmcsa_current_authority_history (is_broker_authority);
