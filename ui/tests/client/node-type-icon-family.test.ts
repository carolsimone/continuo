import { describe, expect, it } from 'vitest';
import { nodeTypeFamily } from '../../src/client/NodeTypeIcon';
import { NODE_TYPES } from '../../src/server/generated/vocabulary.gen';

describe('nodeTypeFamily', () => {
  it('maps python-node to the python family', () => {
    expect(nodeTypeFamily('python-node')).toBe('python');
  });
  it('keeps the csv badge family for python-csv', () => {
    expect(nodeTypeFamily('python-csv')).toBe('python-csv');
  });
  it('maps every dbt kind to the dbt family', () => {
    for (const t of ['dbt-model', 'dbt-seed', 'dbt-snapshot', 'dbt-test']) {
      expect(nodeTypeFamily(t)).toBe('dbt');
    }
  });
  it('renders no icon for an unknown or retired node type', () => {
    expect(nodeTypeFamily('python-model')).toBeNull();
    expect(nodeTypeFamily('')).toBeNull();
  });
  it('classifies every declared node type', () => {
    for (const t of NODE_TYPES) {
      expect(nodeTypeFamily(t)).not.toBeNull();
    }
  });
});
