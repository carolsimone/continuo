import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import path from 'path';
import { BINDING_FIELDS } from '../../src/server/auth/ci-config';

// The chart's values schema and the ui's parser must accept the same binding
// fields, or the chart could accept a value the ui refuses at boot (or the
// reverse).
describe('ciAuth binding fields', () => {
  it('values.schema.json and ci-config.ts agree', () => {
    const schema = JSON.parse(readFileSync(path.resolve(__dirname, '../../../deploy/continuo/values.schema.json'), 'utf8'));
    const item = schema.properties.ciAuth.properties.bindings.additionalProperties.items;
    expect(Object.keys(item.properties).sort()).toEqual([...BINDING_FIELDS].sort());
    expect(item.required).toEqual(['repositoryId']);
    expect(item.additionalProperties).toBe(false);
  });
});
