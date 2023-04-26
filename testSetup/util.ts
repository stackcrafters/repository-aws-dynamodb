import { chunkArray } from '../lib/utils/array';

export const buildBatchRequests = (records, operation, prop = 'Item') => {
  const entries = Object.entries(records).reduce<[string, any[]][]>((acc, [tableName, items]: [string, any]) => {
    items.map((i) => acc.push([tableName, i]));
    return acc;
  }, []);

  return chunkArray(entries, 25).map((chunk) =>
    chunk.reduce((acc, [tableName, item]) => {
      acc[tableName] = acc[tableName] || [];
      acc[tableName].push({
        [operation]: {
          [prop]: item
        }
      });
      return acc;
    }, {})
  );
};
