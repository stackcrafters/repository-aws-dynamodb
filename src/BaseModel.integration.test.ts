import BaseModel from './BaseModel';

class ModelK1 extends BaseModel<any> {
  constructor() {
    super({
      tableName: 'dynamo-integ-test-k1',
      keys: {
        hashKey: 'id'
      },
      stage: 'none'
    });
  }
}
const Model1 = new ModelK1();
class ModelK2 extends BaseModel<any> {
  constructor() {
    super({
      tableName: 'dynamo-integ-test-k2',
      keys: {
        hashKey: 'id',
        rangeKey: 'sort',
        globalIndexes: {
          idx: {
            hashKey: 'id',
            rangeKey: 'idxValue'
          }
        }
      },
      stage: 'none'
    });
  }
}
const Model2 = new ModelK2();

describe('get', () => {
  it('returns item with rangeKey only', async () => {
    const res = await Model1.get({ id: 'test-get-1' });
    expect(res).toEqual({ id: 'test-get-1', value: 1 });
  });
  it('returns item with rangeKey and hashKey', async () => {
    const res = await Model2.get({ id: 'test-get-2', sort: 99 });
    expect(res).toEqual({ id: 'test-get-2', sort: 99, value: 2 });
  });
  it('returns undefined for a non-existent item', async () => {
    const res = await Model1.get({ id: 'non-existent-1' });
    expect(res).toBeUndefined();
  });
});

describe('remove', () => {
  it('deletes item with rangeKey only', async () => {
    const key = { id: 'test-remove-1' };
    const res = await Model1.get({ id: 'test-remove-1' });
    expect(res).toEqual({ ...key, value: 1 });
    await Model1.remove(key);
    const res2 = await Model1.get(key);
    expect(res2).toBeUndefined();
  });
  it('deletes item with rangeKey and hashKey', async () => {
    const key = { id: 'test-remove-2', sort: 98 };
    const res = await Model2.get(key);
    expect(res).toEqual({ ...key, value: 2 });
    await Model2.remove(key);
    const res2 = await Model2.get(key);
    expect(res2).toBeUndefined();
  });
  it('does not throw an error when an item does not exist', async () => {
    await Model1.remove({ id: 'non-existent-1' });
  });
});

describe('removeBatch', () => {
  it('deletes multiple items with rangeKey only', async () => {
    const keys = ['test-batch-remove-1', 'test-batch-remove-2'].map((id) => ({ id }));
    let res = await Model1.getBatch(keys);
    expect(res).toContainEqual({ ...keys[0], value: 1 });
    expect(res).toContainEqual({ ...keys[1], value: 1 });
    await Model1.removeBatch(keys);
    res = await Model1.getBatch(keys);
    expect(res.length).toBe(0);
  });
  it('deletes multiple items with rangeKey and hashKeys', async () => {
    const keys = [
      { id: 'test-batch-remove-3', sort: 97 },
      { id: 'test-batch-remove-4', sort: 97 }
    ];
    let res = await Model2.getBatch(keys);
    expect(res).toContainEqual({ ...keys[0], value: 2 });
    expect(res).toContainEqual({ ...keys[1], value: 2 });
    await Model2.removeBatch(keys);
    res = await Model2.getBatch(keys);
    expect(res.length).toBe(0);
  });
  it('does not throw an error when an item does not exist', async () => {
    const keys = [{ id: 'non-existent-1' }, { id: 'non-existent-2' }];
    const res = await Model1.getBatch(keys);
    expect(res.length).toBe(0);
    await Model1.removeBatch(keys);
  });
});

describe('getBatch', () => {
  it('retrieves multiple items with rangeKey only', async () => {
    const keys = ['test-get-batch-1', 'test-get-batch-2'].map((id) => ({ id }));
    const res = await Model1.getBatch(keys);
    expect(res.length).toBe(2);
    expect(res).toContainEqual({ ...keys[0], value: 1 });
    expect(res).toContainEqual({ ...keys[1], value: 1 });
  });
  it('retrieves multiple items with rangeKey and hashKeys', async () => {
    const keys = ['test-get-batch-2', 'test-get-batch-3'].map((id) => ({ id, sort: 96 }));
    const res = await Model2.getBatch(keys);
    expect(res.length).toBe(2);
    expect(res).toContainEqual({ ...keys[0], value: 2 });
    expect(res).toContainEqual({ ...keys[1], value: 2 });
  });
  it('does not throw an error when an item does not exist', async () => {
    const keys = ['test-get-batch-1', 'non-existent-1'].map((id) => ({ id }));
    const res = await Model1.getBatch(keys);
    expect(res.length).toBe(1);
    expect(res).toContainEqual({ ...keys[0], value: 1 });
  });
});

describe('save', () => {
  it('persists an item without rangeKey', async () => {
    const obj = { id: 'test-create-1' };
    expect(await Model1.get(obj)).toBeUndefined();
    await Model1.save(obj);
    expect(await Model1.get(obj)).toEqual({ ...obj, version: 1 });
  });
  it('persists an item with rangeKey', async () => {
    const obj = { id: 'test-create-2', sort: 95 };
    expect(await Model2.get(obj)).toBeUndefined();
    await Model2.save(obj);
    expect(await Model2.get(obj)).toEqual({ ...obj, version: 1 });
  });
  it('updates an existing item and returns previous version', async () => {
    const obj = { id: 'test-put-1', x: 1, version: 1 };
    expect((await Model1.get(obj)).x).toBeUndefined();
    const res = await Model1.save(obj);
    expect(res).toEqual({ id: 'test-put-1', version: 1 });
    expect(await Model1.get(obj)).toEqual(expect.objectContaining({ x: 1, version: 2 }));
  });
  it('updates an existing item with rangeKey and returns previous version', async () => {
    const obj = { id: 'test-put-2', sort: 95, x: 1, version: 1 };
    expect((await Model2.get(obj)).x).toBeUndefined();
    const res = await Model2.save(obj);
    expect(res).toEqual({ id: 'test-put-2', sort: 95, version: 1 });
    expect(await Model2.get(obj)).toEqual(expect.objectContaining({ x: 1, version: 2 }));
  });
  it('sets dateUpdated when present in the schema', async () => {
    const obj = { id: 'test-create-3', dateUpdated: 1 };
    expect(await Model1.get(obj)).toBeUndefined();
    await Model1.save(obj);
    expect((await Model1.get(obj)).dateUpdated).toBeGreaterThan(1);
  });
  it('sets updatedBy when provided and present in the schema', async () => {
    const obj = { id: 'test-create-4', updatedBy: 1 };
    expect(await Model1.get(obj)).toBeUndefined();
    await Model1.save(obj, 101);
    expect((await Model1.get(obj)).updatedBy).toEqual(101);
  });
  it('applies passing conditionExpression', async () => {
    const obj = { id: 'test-create-5' };
    expect(await Model1.get(obj)).toBeUndefined();
    await Model1.save(obj, 101, 'attribute_not_exists(id)');
    expect(await Model1.get(obj)).toEqual({ ...obj, version: 1 });
  });
  it('applies failing conditionExpression', async () => {
    const obj = { id: 'test-put-3' };
    expect(await Model1.get(obj)).toBeDefined();
    const throws = async () => {
      await Model1.save(obj, 101, 'attribute_not_exists(id)');
    };
    await expect(throws()).rejects.toThrow('The conditional request failed');
    expect((await Model1.get(obj)).version).toBeUndefined();
  });
  it('aborts save on version conflict', async () => {
    const obj = { id: 'test-put-4', version: 1 };
    expect((await Model1.get(obj)).version).toEqual(2);
    const throws = async () => {
      await Model1.save(obj);
    };
    await expect(throws()).rejects.toThrow('The conditional request failed');
    expect((await Model1.get(obj)).version).toEqual(2);
  });
});

describe('saveBatch', () => {
  it('persists multiple items without rangeKey', async () => {
    const objs = ['test-create-batch-1', 'test-create-batch-2'].map((id) => ({ id }));

    await Model1.saveBatch(objs);

    const res = await Model1.getBatch(objs);
    expect(res).toContainEqual(objs[0]);
    expect(res).toContainEqual(objs[1]);
  });
  it('persists multiple items with rangeKey', async () => {
    const objs = ['test-create-batch-1', 'test-create-batch-2'].map((id) => ({ id, sort: 94 }));

    await Model2.saveBatch(objs);

    const res = await Model2.getBatch(objs);
    expect(res).toContainEqual(objs[0]);
    expect(res).toContainEqual(objs[1]);
  });
  it('overwrites multiple items', async () => {
    const objs = ['test-overwrite-1', 'test-overwrite-2'].map((id) => ({ id, x: 1 }));

    const res = await Model1.getBatch(objs);
    expect(res.length).toEqual(2);
    expect(res[0].x).toBeUndefined();
    expect(res[1].x).toBeUndefined();

    await Model1.saveBatch(objs);

    const res1 = await Model1.getBatch(objs);
    expect(res1).toContainEqual(objs[0]);
    expect(res1).toContainEqual(objs[1]);
  });
  it('sets dateUpdated when present in the schema', async () => {
    const objs = [{ id: 'test-overwrite-3', dateUpdated: 1 }];

    const res = await Model1.get(objs[0]);
    expect(res.dateUpdated).toBeUndefined();

    await Model1.saveBatch(objs);

    const res1 = await Model1.get(objs[0]);
    expect(res1.dateUpdated).toBeGreaterThan(1);
  });
  it('sets updatedBy when provided and present in the schema', async () => {
    const objs = [{ id: 'test-overwrite-4', updatedBy: 1 }];

    const res = await Model1.get(objs[0]);
    expect(res.updatedBy).toBeUndefined();

    await Model1.saveBatch(objs, 101);

    const res1 = await Model1.get(objs[0]);
    expect(res1.updatedBy).toEqual(101);
  });
});

describe('updateV2', () => {
  it('creates object if it does not exist with the provided key', async () => {
    const obj = { id: 'test-update-create-1' };

    await Model1.updateV2(obj, { value: 1 }, {}, '101');

    expect(await Model1.get(obj)).toEqual({
      ...obj,
      value: 1,
      createdBy: '101',
      updatedBy: '101',
      dateCreated: expect.any(Number),
      dateUpdated: expect.any(Number),
      version: 1
    });
  });
  it('updates existing object', async () => {
    const obj = { id: 'test-update-1', sort: 93, version: 1 };

    await Model2.updateV2(obj, { value: 2 }, {}, '101');

    const res = await Model2.get(obj);
    expect(res).toEqual({
      ...obj,
      value: 2,
      createdBy: '100',
      updatedBy: '101',
      dateCreated: 1,
      dateUpdated: expect.any(Number),
      version: 2
    });
    expect(res.dateUpdated).toBeGreaterThan(1);
  });
  it('throws error on version mismatch', async () => {
    const obj = { id: 'test-update-2', version: 0 };

    const throws = async () => {
      await Model1.updateV2(obj, { value: 1 });
    };

    await expect(throws).rejects.toThrow('The conditional request failed');
  });
  it('sets version on object if not exists', async () => {
    const obj = { id: 'test-update-3' };

    await Model1.updateV2(obj, { value: 1 });

    const res = await Model1.get(obj);
    expect(res.version).toEqual(1);
  });
  it('overwrites object on version mismatch when skipVersionCondition is true', async () => {
    const obj = { id: 'test-update-4', version: 0 };

    await Model1.updateV2(obj, { value: 1 }, { skipVersionCondition: true });

    const res = await Model1.get(obj);
    expect(res.value).toEqual(1);
    expect(res.version).toEqual(2);
  });
  it('applies provided ConditionExpressions', async () => {
    const obj = { id: 'test-update-5' };

    const throws = async () => {
      await Model1.updateV2(obj, { value: 2 }, { opts: { ConditionExpression: '#k_value <> :v_1' } });
    };

    await expect(throws).rejects.toThrow('The conditional request failed');
  });
  it('allows expression attribute names and values to be overridden', async () => {
    const obj = { id: 'test-update-6' };

    const res = await Model1.updateV2(obj, { value: 1 }, { opts: { ExpressionAttributeValues: { ':v_value': 2 } } });

    expect(res.value).toEqual(2);
  });
  it('returns new object (ALL_NEW set as ReturnValues) by default', async () => {
    const obj = { id: 'test-update-7' };

    const res = await Model1.updateV2(obj, { value: 1 }, {}, '101');

    expect(res).toEqual({
      ...obj,
      value: 1,
      createdBy: '101',
      updatedBy: '101',
      dateCreated: expect.any(Number),
      dateUpdated: expect.any(Number),
      version: 1
    });
  });
});

describe('query', () => {
  const obj = { id: 'test-query' };
  it('returns list of items for query', async () => {
    const res = await Model2.query(obj);

    expect(res.items).toEqual([1, 2, 3, 4].map((n) => ({ ...obj, sort: n })));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
  it('allows overriding of page size via Limit', async () => {
    const res = await Model2.query(obj, { maxRequests: 1, opts: { Limit: 2 } });

    expect(res.items.length).toEqual(2);
    expect(res.lastEvaluatedKey).toEqual({ ...obj, sort: 2 });
  });
  it('returns paginated list of items for query', async () => {
    const res = await Model2.query(obj, { opts: { Limit: 2 } });

    expect(res.items).toEqual([1, 2, 3, 4].map((n) => ({ ...obj, sort: n })));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
  it('allows resumption of pagination part-way through a query', async () => {
    const res = await Model2.query(obj, { maxRequests: 1, opts: { Limit: 2 } });

    expect(res.items).toEqual([1, 2].map((n) => ({ ...obj, sort: n })));

    const secondRes = await Model2.query(obj, { opts: { ExclusiveStartKey: res.lastEvaluatedKey } });

    expect(secondRes.items).toEqual([3, 4].map((n) => ({ ...obj, sort: n })));
  });
  it('allows use of custom rangeOp', async () => {
    const res = await Model2.query({ ...obj, sort: 3 }, { rangeOp: '>=' });

    expect(res.items).toEqual([3, 4].map((n) => ({ ...obj, sort: n })));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
});

describe('queryIndex', () => {
  const obj = { id: 'test-queryIndex' };
  it('returns list of items for index', async () => {
    const res = await Model2.queryIndex('idx', { ...obj, idxValue: 'xyz' });

    expect(res.items).toEqual([3, 4].map((n) => ({ ...obj, sort: n, idxValue: 'xyz' })));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
  it('allows overriding of page size via Limit', async () => {
    const res = await Model2.queryIndex('idx', { ...obj, idxValue: 'xyz' }, { maxRequests: 1, opts: { Limit: 1 } });

    expect(res.items.length).toEqual(1);
    expect(res.lastEvaluatedKey).toEqual({ ...obj, sort: 3, idxValue: 'xyz' });
  });
  it('returns paginated list of items for query', async () => {
    const res = await Model2.queryIndex('idx', { ...obj }, { opts: { Limit: 2 } });

    expect(res.items).toEqual(
      expect.arrayContaining([
        { ...obj, sort: 1, idxValue: 'abc' },
        { ...obj, sort: 2, idxValue: 'abc' },
        { ...obj, sort: 3, idxValue: 'xyz' },
        { ...obj, sort: 4, idxValue: 'xyz' }
      ])
    );
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
  it('allows resumption of pagination part-way through a query', async () => {
    const res = await Model2.queryIndex('idx', obj, { maxRequests: 1, opts: { Limit: 2 } });

    expect(res.items).toEqual(expect.arrayContaining([1, 2].map((n) => ({ ...obj, sort: n, idxValue: 'abc' }))));

    const secondRes = await Model2.queryIndex('idx', obj, { opts: { ExclusiveStartKey: res.lastEvaluatedKey } });

    expect(secondRes.items).toEqual([3, 4].map((n) => ({ ...obj, sort: n, idxValue: 'xyz' })));
  });
  it('allows use of custom rangeOp', async () => {
    const res = await Model2.queryIndex('idx', { ...obj, value: 'xyz' }, { rangeOp: '=' });

    expect(res.items).toEqual(expect.arrayContaining([3, 4].map((n) => ({ ...obj, sort: n, idxValue: 'xyz' }))));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
  it('allows use of begins_with rangeOp', async () => {
    const res = await Model2.queryIndex('idx', { ...obj, value: 'ab' }, { rangeOp: 'begins_with' });

    expect(res.items).toEqual(expect.arrayContaining([1, 2].map((n) => ({ ...obj, sort: n, idxValue: 'abc' }))));
    expect(res.lastEvaluatedKey).toBeUndefined();
  });
});

describe('all', () => {
  it('returns all table items (within requestLimit)', async () => {
    const res = await Model2.all();

    expect(res?.length).toBeGreaterThan(1);
    expect(res?.[0]).toEqual(expect.objectContaining({ id: expect.any(String) }));
  });
});
