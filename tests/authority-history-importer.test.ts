import {
  buildAuthorityHistoryInsertBatch,
  mapAuthorityHistoryRow,
  parseAuthorityHistoryDate,
  parseCsvLine,
} from '../src/importers/authorityHistoryImporter';

const baseFields = [
  'mc123456',
  '123456',
  '',
  'BROKER',
  'GRANTED',
  '01/02/2024',
  '',
  '',
  '',
];

function map(fields: string[] = baseFields) {
  const row = mapAuthorityHistoryRow(fields);

  if (!row) {
    throw new Error('Expected row to map successfully.');
  }

  return row;
}

describe('authority history importer', () => {
  it('parses quoted TXT row correctly', () => {
    expect(parseCsvLine('"MC123456","00123456","","PROPERTY BROKER","GRANTED, PENDING","01/02/2024","","",""')).toEqual([
      'MC123456',
      '00123456',
      '',
      'PROPERTY BROKER',
      'GRANTED, PENDING',
      '01/02/2024',
      '',
      '',
      '',
    ]);
  });

  it('maps columns correctly', () => {
    const row = map([
      'MC777777',
      '7654321',
      'A',
      'CONTRACT',
      'REINSTATED',
      '02/03/2024',
      'ADMINISTRATIVE INACTIVATION',
      '04/05/2024',
      '06/07/2024',
    ]);

    expect(row).toMatchObject({
      docket_number: 'MC777777',
      dot_number: '07654321',
      sub_number: 'A',
      authority_type: 'CONTRACT',
      original_action: 'REINSTATED',
      original_action_date: '2024-02-03',
      final_action: 'ADMINISTRATIVE INACTIVATION',
      final_decision_date: '2024-04-05',
      final_served_date: '2024-06-07',
    });
  });

  it('normalizes DOT to 8 digits', () => {
    expect(map().dot_number).toBe('00123456');
  });

  it('normalizes docket number to uppercase', () => {
    expect(map().docket_number).toBe('MC123456');
  });

  it('converts empty strings to null', () => {
    const row = map();

    expect(row.sub_number).toBeNull();
    expect(row.final_action).toBeNull();
    expect(row.final_decision_date).toBeNull();
    expect(row.final_served_date).toBeNull();
  });

  it('parses MM/DD/YYYY dates to YYYY-MM-DD', () => {
    expect(parseAuthorityHistoryDate('12/31/2025')).toBe('2025-12-31');
    expect(parseAuthorityHistoryDate('02/30/2025')).toBeNull();
  });

  it('detects BROKER / PROPERTY BROKER as broker authority', () => {
    expect(map([...baseFields.slice(0, 3), 'BROKER', ...baseFields.slice(4)]).is_broker_authority).toBe(
      true,
    );
    expect(
      map([...baseFields.slice(0, 3), 'PROPERTY BROKER', ...baseFields.slice(4)])
        .is_broker_authority,
    ).toBe(true);
  });

  it('detects COMMON / CONTRACT / MOTOR PROPERTY as carrier authority', () => {
    expect(map([...baseFields.slice(0, 3), 'COMMON', ...baseFields.slice(4)]).is_carrier_authority).toBe(
      true,
    );
    expect(
      map([...baseFields.slice(0, 3), 'CONTRACT', ...baseFields.slice(4)]).is_carrier_authority,
    ).toBe(true);
    expect(
      map([...baseFields.slice(0, 3), 'MOTOR PROPERTY', ...baseFields.slice(4)])
        .is_carrier_authority,
    ).toBe(true);
  });

  it('detects ADMINISTRATIVE INACTIVATION as negative final action', () => {
    expect(
      map([...baseFields.slice(0, 6), 'ADMINISTRATIVE INACTIVATION', ...baseFields.slice(7)])
        .is_negative_final_action,
    ).toBe(true);
  });

  it('detects DISCONTINUED REVOCATION flag but does not classify it as hard active revocation', () => {
    const row = map([...baseFields.slice(0, 6), 'DISCONTINUED REVOCATION', ...baseFields.slice(7)]);

    expect(row.is_discontinued_revocation).toBe(true);
    expect(row.is_revoked).toBe(false);
    expect(row.is_negative_final_action).toBe(false);
  });

  it('builds INSERT batch with ON CONFLICT DO NOTHING', () => {
    const batch = buildAuthorityHistoryInsertBatch([map()]);

    expect(batch.text).toContain('INSERT INTO fmcsa_authority_history');
    expect(batch.text).toContain('ON CONFLICT DO NOTHING');
    expect(batch.text).toContain('$16::jsonb');
    expect(batch.values).toHaveLength(16);
  });
});
