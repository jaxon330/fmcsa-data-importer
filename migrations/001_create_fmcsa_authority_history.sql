CREATE TABLE IF NOT EXISTS fmcsa_authority_history (
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
  raw_record JSONB,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fmcsa_authority_history_unique_record UNIQUE (
    docket_number,
    dot_number,
    sub_number,
    authority_type,
    original_action,
    original_action_date,
    final_action,
    final_decision_date,
    final_served_date
  )
);

CREATE INDEX IF NOT EXISTS idx_fmcsa_authority_history_dot_number
  ON fmcsa_authority_history (dot_number);

CREATE INDEX IF NOT EXISTS idx_fmcsa_authority_history_docket_number
  ON fmcsa_authority_history (docket_number);

CREATE INDEX IF NOT EXISTS idx_fmcsa_authority_history_is_broker_authority
  ON fmcsa_authority_history (is_broker_authority);

CREATE INDEX IF NOT EXISTS idx_fmcsa_authority_history_dot_docket_broker
  ON fmcsa_authority_history (dot_number, docket_number, is_broker_authority);
