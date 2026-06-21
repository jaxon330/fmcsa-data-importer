import type { Pool } from 'pg';

const CURRENT_TABLES = [
  'fmcsa_current_carriers',
  'fmcsa_current_active_pending_insurance',
  'fmcsa_current_insurance_history',
  'fmcsa_current_revocations',
  'fmcsa_current_authority_history',
] as const;

export const BASELINE_COPY_STATEMENTS = [
  `
    INSERT INTO fmcsa_current_carriers (
      docket_number, dot_number, legal_name, dba_name, broker_stat, common_stat, contract_stat,
      broker_app_pend, broker_rev_pend, bond_req, bond_file, bipd_file, cargo_file,
      bus_street_po, bus_city, bus_state_code, bus_ctry_code, bus_zip_code, bus_telno,
      mail_street_po, mail_city, mail_state_code, mail_ctry_code, mail_zip_code,
      canonical_key, source_provider, source_priority, last_legacy_seen_at, last_motus_seen_at,
      source_record_hash, raw_record, imported_at, updated_at
    )
    SELECT DISTINCT ON (canonical_key)
      docket_number, dot_number, legal_name, dba_name, broker_stat, common_stat, contract_stat,
      broker_app_pend, broker_rev_pend, bond_req, bond_file, bipd_file, cargo_file,
      bus_street_po, bus_city, bus_state_code, bus_ctry_code, bus_zip_code, bus_telno,
      mail_street_po, mail_city, mail_state_code, mail_ctry_code, mail_zip_code,
      canonical_key, 'legacy', 0, now(), NULL, source_record_hash, raw_record, imported_at, updated_at
    FROM (
      SELECT legacy.*,
        upper(coalesce(docket_number, '')) || '|' || upper(coalesce(dot_number, '')) AS canonical_key
      FROM fmcsa_carriers legacy
    ) baseline
    ORDER BY canonical_key, updated_at DESC, id DESC
  `,
  `
    INSERT INTO fmcsa_current_active_pending_insurance (
      docket_number, dot_number, ins_form_code, insurance_type_description, insurance_company_name,
      policy_no, posted_date, effective_date, cancel_effective_date,
      canonical_key, source_provider, source_priority, last_legacy_seen_at, last_motus_seen_at,
      source_record_hash, raw_record, imported_at, updated_at
    )
    SELECT DISTINCT ON (canonical_key)
      docket_number, dot_number, ins_form_code, insurance_type_description, insurance_company_name,
      policy_no, posted_date, effective_date, cancel_effective_date,
      canonical_key, 'legacy', 0, now(), NULL, source_record_hash, raw_record, imported_at, updated_at
    FROM (
      SELECT legacy.*,
        upper(coalesce(docket_number, '')) || '|' ||
        upper(coalesce(dot_number, '')) || '|' ||
        upper(coalesce(ins_form_code, '')) || '|' ||
        upper(coalesce(policy_no, '')) || '|' ||
        upper(coalesce(effective_date::text, '')) AS canonical_key
      FROM fmcsa_active_pending_insurance legacy
    ) baseline
    ORDER BY canonical_key, updated_at DESC, id DESC
  `,
  `
    INSERT INTO fmcsa_current_insurance_history (
      docket_number, dot_number, ins_form_code, cancellation_method, insurance_type_description,
      policy_no, effective_date, cancel_effective_date, specific_cancellation_method,
      insurance_company_name, canonical_key, source_provider, source_priority,
      last_legacy_seen_at, last_motus_seen_at, source_record_hash, raw_record, imported_at, updated_at
    )
    SELECT DISTINCT ON (canonical_key)
      docket_number, dot_number, ins_form_code, cancellation_method, insurance_type_description,
      policy_no, effective_date, cancel_effective_date, specific_cancellation_method,
      insurance_company_name, canonical_key, 'legacy', 0, now(), NULL,
      source_record_hash, raw_record, imported_at, updated_at
    FROM (
      SELECT legacy.*,
        upper(coalesce(docket_number, '')) || '|' ||
        upper(coalesce(dot_number, '')) || '|' ||
        upper(coalesce(ins_form_code, '')) || '|' ||
        upper(coalesce(policy_no, '')) || '|' ||
        upper(coalesce(effective_date::text, '')) || '|' ||
        upper(coalesce(cancel_effective_date::text, '')) AS canonical_key
      FROM fmcsa_insurance_history legacy
    ) baseline
    ORDER BY canonical_key, updated_at DESC, id DESC
  `,
  `
    INSERT INTO fmcsa_current_revocations (
      docket_number, dot_number, authority_type, serve_date, revocation_type, effective_date,
      canonical_key, source_provider, source_priority, last_legacy_seen_at, last_motus_seen_at,
      source_record_hash, raw_record, imported_at, updated_at
    )
    SELECT DISTINCT ON (canonical_key)
      docket_number, dot_number, authority_type, serve_date, revocation_type, effective_date,
      canonical_key, 'legacy', 0, now(), NULL, source_record_hash, raw_record, imported_at, updated_at
    FROM (
      SELECT legacy.*,
        upper(coalesce(docket_number, '')) || '|' ||
        upper(coalesce(dot_number, '')) || '|' ||
        upper(coalesce(authority_type, '')) || '|' ||
        upper(coalesce(effective_date::text, '')) AS canonical_key
      FROM fmcsa_revocations legacy
    ) baseline
    ORDER BY canonical_key, updated_at DESC, id DESC
  `,
  `
    INSERT INTO fmcsa_current_authority_history (
      docket_number, dot_number, sub_number, authority_type, original_action, original_action_date,
      final_action, final_decision_date, final_served_date, is_broker_authority,
      is_carrier_authority, is_negative_final_action, is_revoked, is_reinstated,
      is_discontinued_revocation, canonical_key, source_provider, source_priority,
      last_legacy_seen_at, last_motus_seen_at, source_record_hash, raw_record, imported_at, updated_at
    )
    SELECT DISTINCT ON (canonical_key)
      docket_number, dot_number, sub_number, authority_type, original_action, original_action_date,
      final_action, final_decision_date, final_served_date, is_broker_authority,
      is_carrier_authority, is_negative_final_action, is_revoked, is_reinstated,
      is_discontinued_revocation, canonical_key, 'legacy', 0, now(), NULL,
      source_record_hash, raw_record, imported_at, updated_at
    FROM (
      SELECT legacy.*,
        upper(coalesce(docket_number, '')) || '|' ||
        upper(coalesce(dot_number, '')) || '|' ||
        upper(coalesce(authority_type, '')) || '|' ||
        upper(coalesce(final_action, '')) || '|' ||
        upper(coalesce(final_decision_date::text, '')) AS canonical_key
      FROM fmcsa_authority_history legacy
    ) baseline
    ORDER BY canonical_key, updated_at DESC, id DESC
  `,
] as const;

export async function rebuildCurrentServingBaseline(pool: Pick<Pool, 'connect'>): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`TRUNCATE TABLE ${CURRENT_TABLES.join(', ')} RESTART IDENTITY`);
    for (const statement of BASELINE_COPY_STATEMENTS) {
      await client.query(statement);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
