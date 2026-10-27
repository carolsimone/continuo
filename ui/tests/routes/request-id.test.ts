import { describe, it, expect } from 'vitest';
import { isRequestId, REQUEST_ID_MESSAGE } from '../../src/server/routes/request-id';

describe('request-id: isRequestId', () => {
  it.each([
    '5d21c0de-0000-4000-8000-000000000001',
    '5D21C0DE-0000-4000-8000-00000000000A',
    '00000000-0000-0000-0000-000000000000',
  ])('accepts the UUID %s', (v) => {
    expect(isRequestId(v)).toBe(true);
  });

  it.each([
    '', ' ', 'not-a-uuid', '5d21c0de000040008000000000000001',
    '5d21c0de-0000-4000-8000-00000000000', '5d21c0de-0000-4000-8000-0000000000012',
    '{5d21c0de-0000-4000-8000-000000000001}', ' 5d21c0de-0000-4000-8000-000000000001',
    'gd21c0de-0000-4000-8000-000000000001', 7, null, undefined, {}, ['5d21c0de-0000-4000-8000-000000000001'],
  ])('refuses %j', (v) => {
    expect(isRequestId(v)).toBe(false);
  });

  it('words its refusal for the browser', () => {
    expect(REQUEST_ID_MESSAGE).toBe('request_id must be a UUID');
  });
});
