import { mockClient } from 'aws-sdk-client-mock';
import 'aws-sdk-client-mock-jest';
import BaseModel, { BaseObject } from './BaseModel';
import {
  BatchGetCommand,
  BatchGetCommandInput,
  BatchWriteCommand,
  BatchWriteCommandInput,
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  UpdateCommand
} from '@aws-sdk/lib-dynamodb';

const config = {
  tableName: 'test',
  keys: {
    hashKey: 'id',
    rangeKey: 'range',
    globalIndexes: {
      'dateCreated-index': { hashKey: 'dateCreated' },
      'createdBy-dateCreated-index': { hashKey: 'createdBy', rangeKey: 'dateCreated' }
    }
  }
};

interface Test extends BaseObject {
  id: string;
  range: string;
}

class TestModel extends BaseModel<Test> {
  constructor() {
    super(config);
  }
}

const instance = new TestModel();
Object.freeze(instance);

class TestModelPostfix extends BaseModel<Test> {
  constructor() {
    super({ ...config, stage: 'postfix' });
  }
}

const postfixInstance = new TestModelPostfix();
Object.freeze(postfixInstance);

class TestModelNoStage extends BaseModel<Test> {
  constructor() {
    super({ ...config, stage: 'none' });
  }
}

const noStageInstance = new TestModelNoStage();
Object.freeze(noStageInstance);

const ddbMock = mockClient(DynamoDBDocumentClient);

beforeEach(() => {
  jest.resetModules();
  jest.resetAllMocks();
  ddbMock.reset();
});

const setupPaginatedResults = (pageCount = 8) => {
  const pageSize = 10;
  const totalItemCount = pageCount * pageSize;
  let allItems: any = [];
  const stub = ddbMock.on(QueryCommand);
  for (let i = 0; i < totalItemCount; i += pageSize) {
    const batch = [...Array(pageSize).keys()].map((n) => ({ id: `k${i + n}`, range: `r${i + n}` }));
    stub.resolvesOnce({ Items: batch, ...(i + pageSize < totalItemCount ? { LastEvaluatedKey: batch[batch.length - 1] } : {}) });
    allItems = allItems.concat(batch);
  }
  return allItems;
};

describe('get', () => {
  const item = {};
  beforeEach(() => {
    ddbMock.on(GetCommand).resolves({ Item: item });
  });

  it('queries the correct table name (prefix)', async () => {
    await instance.get({ id: 'id1', range: 'r1' });
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, { TableName: 'stg-test' });
  });
  it('queries the correct table name (postfix)', async () => {
    await postfixInstance.get({ id: 'id1', range: 'r1' });
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, { TableName: 'test-stg' });
  });
  it('queries the correct table name (no stage)', async () => {
    await noStageInstance.get({ id: 'id1', range: 'r1' });
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, { TableName: 'test' });
  });
  it('queries using the object hash and range keys', async () => {
    await instance.get({ id: 'k1', range: 'r1', unrelated: 'pickles' });
    expect(ddbMock).toHaveReceivedCommandWith(GetCommand, {
      Key: {
        id: 'k1',
        range: 'r1'
      }
    });
  });
  it('returns the item from the response', async () => {
    const res = await instance.get({ id: 'k1', range: 'r1' });
    expect(res).toEqual(item);
  });
  it('returns undefined when the item does not exist', async () => {
    ddbMock.on(GetCommand).rejects(new Error('err'));

    const result = await instance.get({ id: 'k1', range: 'r1' });

    expect(result).toBeUndefined();
  });
});

describe('remove', () => {
  beforeEach(() => {
    ddbMock.on(DeleteCommand).resolves({});
  });
  it('queries the correct table name', async () => {
    await instance.remove({ id: 'k1', range: 'r1' });
    expect(ddbMock).toHaveReceivedCommandWith(DeleteCommand, { TableName: 'stg-test' });
  });
  it('queries using the object hash and range keys', async () => {
    await instance.remove({ id: 'k1', range: 'r1', unrelated: 'pickles' });
    expect(ddbMock).toHaveReceivedCommandWith(DeleteCommand, {
      Key: {
        id: 'k1',
        range: 'r1'
      }
    });
  });
});

describe('removeBatch', () => {
  beforeEach(() => {
    ddbMock.on(BatchWriteCommand).resolves({});
  });
  it('queries the correct table name', async () => {
    const args = [
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ];
    await instance.removeBatch(args);
    expect(ddbMock).toHaveReceivedCommandWith(BatchWriteCommand, {
      RequestItems: { 'stg-test': args.map((a) => ({ DeleteRequest: { Key: a } })) }
    });
  });
  it('batches requests into sets of 25', async () => {
    const objs = [...Array(30).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    await instance.removeBatch(objs);

    expect(ddbMock).toHaveReceivedCommandTimes(BatchWriteCommand, 2);
    expect((<BatchWriteCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'].length).toEqual(25);
    expect((<BatchWriteCommandInput>ddbMock.call(1).args[0].input).RequestItems?.['stg-test'].length).toEqual(5);
  });
  it('queries using the object hash and range keys', async () => {
    await instance.removeBatch([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);

    expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);
    expect((<BatchWriteCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test']).toEqual([
      { DeleteRequest: { Key: { id: 'k1', range: 'r1' } } },
      { DeleteRequest: { Key: { id: 'k2', range: 'r2' } } }
    ]);
  });
  it('throws on promise rejection', async () => {
    ddbMock.on(BatchWriteCommand).rejects(new Error('err'));

    const throws = async () => {
      await instance.removeBatch([{ id: 'k1', range: 'r1' }]);
    };

    await expect(throws).rejects.toThrowError('err');
  });
  it('returns UnprocessedItems', async () => {
    const items = [1234];
    ddbMock.on(BatchWriteCommand).resolves({ UnprocessedItems: { 'stg-test': items } });

    const res = await instance.removeBatch([{ id: 'k1', range: 'r1' }]);

    expect(res).toEqual(items);
  });
});

describe('getBatch', () => {
  beforeEach(() => {
    ddbMock.on(BatchGetCommand).resolves({ Responses: { 'stg-test': [{ key: 1 }] } });
  });
  it('queries the correct table name', async () => {
    await instance.getBatch([{ id: 'k1', range: 'r1' }]);

    expect(ddbMock).toHaveReceivedCommandWith(BatchGetCommand, { RequestItems: { 'stg-test': expect.anything() } });
  });
  it('splits requests into batches of 100', async () => {
    const objs = [...Array(110).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    await instance.getBatch(objs);

    expect(ddbMock).toHaveReceivedCommandTimes(BatchGetCommand, 2);
    expect((<BatchGetCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'].Keys?.length).toEqual(100);
    expect((<BatchGetCommandInput>ddbMock.call(1).args[0].input).RequestItems?.['stg-test'].Keys?.length).toEqual(10);
  });
  it('returns concatenated results', async () => {
    const objs = [...Array(110).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    const res = await instance.getBatch(objs);

    expect(res).toEqual([{ key: 1 }, { key: 1 }]);
  });
  it('queries using object hash and range keys', async () => {
    await instance.getBatch([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);

    expect(ddbMock).toHaveReceivedCommand(BatchGetCommand);
    expect((<BatchGetCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'].Keys).toEqual([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);
  });
  it('throws on promise rejection', async () => {
    ddbMock.on(BatchGetCommand).rejects(new Error('err'));

    const throws = async () => {
      await instance.getBatch([{ id: 'k1', range: 'r1' }]);
    };

    await expect(throws).rejects.toThrowError('err');
  });
});

describe('prepareSave', () => {
  let testObj;
  beforeEach(() => {
    testObj = { id: 'k1', range: 'r1' };
  });
  it("throws error when item doesn't contain a hashKey", () => {
    const throws = () => {
      instance.prepareSave(<Test>{ range: 'r1' });
    };

    expect(throws).toThrowError('item being saved does not contain a valid hashKey (id)');
  });
  it("throws error when a rangeKey is expected and item doesn't contain a rangeKey", () => {
    const throws = () => {
      instance.prepareSave(<Test>{ id: 'k1' });
    };

    expect(throws).toThrowError('item being saved does not contain a valid rangeKey (range)');
  });
  it('sets version condition to attribute_not_exists when version value is absent', () => {
    const res = instance.prepareSave(testObj);

    expect(res.ConditionExpression).toEqual('attribute_not_exists(version)');
  });
  it('sets version condition to current value when version value is present', () => {
    const res = instance.prepareSave({ ...testObj, version: 1 });

    expect(res.ConditionExpression).toEqual('version = :ver');
    expect(res.ExpressionAttributeValues).toEqual({ ':ver': 1 });
  });
  it('increments object version attribute when version value is present', () => {
    const res = instance.prepareSave({ ...testObj, version: 1 });

    expect(res.Item?.version).toEqual(2);
  });
  it('sets object version attribute to 1 when version value is absent', () => {
    const res = instance.prepareSave(testObj);

    expect(res.Item?.version).toEqual(1);
  });
  it('sets dateUpdated to current timestamp when dateUpdated field is present', () => {
    const res = instance.prepareSave({ ...testObj, dateUpdated: 1 });

    expect(res.Item?.dateUpdated).not.toBeUndefined();
    expect(res.Item?.dateUpdated).not.toEqual(1);
  });
  it('sets updatedBy to current user when updatedBy field is present', () => {
    const res = instance.prepareSave({ ...testObj, updatedBy: true }, 101);

    expect(res.Item?.updatedBy).not.toBeUndefined();
    expect(res.Item?.updatedBy).not.toEqual(1);
  });
  it('returns correct table name', () => {
    const res = instance.prepareSave(testObj);

    expect(res.TableName).toEqual('stg-test');
  });
  it('returns the item to save', () => {
    const res = instance.prepareSave(testObj);

    expect(res.Item).toEqual(testObj);
  });
  it('sets ReturnValues to ALL_OLD', () => {
    const res = instance.prepareSave(testObj);

    expect(res.ReturnValues).toEqual('ALL_OLD');
  });
  it('sets ConditionExpression to conditionExpression and versionCondition when condition set', () => {
    const res = instance.prepareSave({ ...testObj, version: 1 }, 101, 'x = 1');

    expect(res.ConditionExpression).toEqual('x = 1 AND version = :ver');
  });
  it('sets ExpressionAttributeValues to original object version values', () => {
    const res = instance.prepareSave({ ...testObj, version: 1 });

    expect(res.ExpressionAttributeValues).toEqual({ ':ver': 1 });
  });
});

describe('save', () => {
  let testObj;
  beforeEach(() => {
    testObj = { id: 'k1', range: 'r1' };
    ddbMock.on(PutCommand).resolves({ Attributes: testObj });
  });
  it('sets item with parameters', async () => {
    await instance.save(testObj);

    expect(ddbMock).toHaveReceivedCommandWith(PutCommand, { Item: testObj });
  });
  it('sets conditionExpression when provided', async () => {
    await instance.save(testObj, 101, 'x = 1');

    expect(ddbMock).toHaveReceivedCommandWith(PutCommand, { ConditionExpression: 'x = 1 AND attribute_not_exists(version)' });
  });
  it('returns the previous saved object', async () => {
    const obj = await instance.save(testObj);

    expect(obj).toEqual(testObj);
  });
});

describe('saveBatch', () => {
  let testObj;
  beforeEach(() => {
    testObj = { id: 'k1', range: 'r1' };
  });
  it('throws error when object does not contain a valid hashKey', async () => {
    const throws = async () => {
      await instance.saveBatch([{}]);
    };

    await expect(throws).rejects.toThrowError('item being saved does not contain a valid hashKey (id)');
  });
  it('throws error when rangeKey is required and object does not contain a valid rangeKey', async () => {
    const throws = async () => {
      await instance.saveBatch([{ id: 'k1' }]);
    };

    await expect(throws).rejects.toThrowError('item being saved does not contain a valid rangeKey (range)');
  });
  it('batches requests into sets of 25', async () => {
    const objs = [...Array(35).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    await instance.saveBatch(objs);

    expect(ddbMock).toHaveReceivedCommandTimes(BatchWriteCommand, 2);
    expect((<BatchWriteCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'].length).toEqual(25);
    expect((<BatchWriteCommandInput>ddbMock.call(1).args[0].input).RequestItems?.['stg-test'].length).toEqual(10);
  });
  it('updates dateUpdated where present in the schema', async () => {
    await instance.saveBatch([{ ...testObj, dateUpdated: 1 }]);

    expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);
    expect(
      (<BatchWriteCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'][0].PutRequest?.Item?.dateUpdated
    ).toBeGreaterThan(1);
  });
  it('updates updatedBy where present in the schema', async () => {
    await instance.saveBatch([{ ...testObj, updatedBy: 1 }], 101);

    expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);
    expect((<BatchWriteCommandInput>ddbMock.call(0).args[0].input).RequestItems?.['stg-test'][0].PutRequest?.Item?.updatedBy).toEqual(101);
  });
});

describe('prepareUpdateV2', () => {
  const item = { id: 'id', range: 'range' };
  it("throws error when item doesn't contain a hashKey", () => {
    const throws = () => {
      instance.prepareUpdateV2({});
    };
    expect(throws).toThrow('item being updated does not contain a valid hashKey (id)');
  });
  it("throws error when a rangeKey is expected and item doesn't contain a rangeKey", () => {
    const throws = () => {
      instance.prepareUpdateV2({ id: 'id' });
    };
    expect(throws).toThrow('item being updated does not contain a valid rangeKey (range)');
  });
  it('sets version condition to attribute_not_exists when version value is absent', () => {
    const res = instance.prepareUpdateV2(item);

    expect(res.ConditionExpression).toEqual('attribute_not_exists(version)');
  });
  it('sets version condition to current value when version value is present', () => {
    const res = instance.prepareUpdateV2({ ...item, version: 1 });

    expect(res.ConditionExpression).toEqual('version = :ver');
    expect(res.ExpressionAttributeValues).toEqual(expect.objectContaining({ ':ver': 1 }));
  });
  it('increments object version attribute', () => {
    const res = instance.prepareUpdateV2({ ...item, version: 1 });

    expect(res.UpdateExpression).toContain('#version = if_not_exists(#version, :v_0) + :v_1');
    expect(res.ExpressionAttributeValues).toEqual(expect.objectContaining({ ':v_0': 0, ':v_1': 1 }));
  });
  it('sets dateUpdated to current timestamp', () => {
    const res = instance.prepareUpdateV2({ ...item, version: 1 });

    expect(res.UpdateExpression).toContain('#dateUpdated = :now');
  });
  it('sets updatedBy to current user when userId provided', () => {
    const res = instance.prepareUpdateV2(item, {}, {}, '101');

    expect(res.UpdateExpression).toContain('#updatedBy = :userId');
  });
  it('sets createdBy to current user when userId provided and new object', () => {
    const res = instance.prepareUpdateV2(item, {}, {}, '101');

    expect(res.UpdateExpression).toContain('#createdBy = if_not_exists(#createdBy, :userId)');
  });
  it('sets dateCreated when new object', () => {
    const res = instance.prepareUpdateV2(item);

    expect(res.UpdateExpression).toContain('#dateCreated = if_not_exists(#dateCreated, :now)');
  });
  it('returns correct table name', () => {
    const res = instance.prepareUpdateV2(item, {}, {}, '101');

    expect(res.TableName).toEqual('stg-test');
  });
  it('sets ReturnValues', () => {
    const res = instance.prepareUpdateV2(item, {}, { opts: { ReturnValues: 'ALL_NEW' } }, '101');

    expect(res.ReturnValues).toEqual('ALL_NEW');
  });
  it('sets ConditionExpression to conditionExpression and versionCondition when condition set', () => {
    const res = instance.prepareUpdateV2(item, {}, { opts: { ConditionExpression: '#k_x = :v_1' } }, '101');

    expect(res.ConditionExpression).toEqual('#k_x = :v_1 AND attribute_not_exists(version)');
  });
  it('sets Key property from object key', () => {
    const res = instance.prepareUpdateV2(item);

    expect(res.Key).toEqual(item);
  });
  it('sets property values to be updated in ExpressionAttribute name and values, UpdateExpression', () => {
    const res = instance.prepareUpdateV2(item, { propName: 1 });

    expect(res.UpdateExpression).toContain('#k_propName = :v_propName');
    expect(res.ExpressionAttributeNames).toEqual(expect.objectContaining({ '#k_propName': 'propName' }));
    expect(res.ExpressionAttributeValues).toEqual(expect.objectContaining({ ':v_propName': 1 }));
  });
  it('skips version check when skipVersionCondition set true, retaining other conditionExpressions', () => {
    const res = instance.prepareUpdateV2(item, {}, { skipVersionCondition: true, opts: { ConditionExpression: '#k_x = :v_1' } });

    expect(res.ConditionExpression).toEqual('#k_x = :v_1');
  });
  it('allows overriding of auto generated ExpressionAttribute names and values', () => {
    const res = instance.prepareUpdateV2(
      item,
      { x: 1 },
      { opts: { ExpressionAttributeNames: { '#k_x': 'name' }, ExpressionAttributeValues: { ':v_x': 'if_not_exists(#k_x, 0)' } } }
    );

    expect(res.ExpressionAttributeNames).toEqual(expect.objectContaining({ '#k_x': 'name' }));
    expect(res.ExpressionAttributeValues).toEqual(expect.objectContaining({ ':v_x': 'if_not_exists(#k_x, 0)' }));
  });
});

describe('updateV2', () => {
  const item = { id: 'id', range: 'range' };
  beforeEach(() => {
    ddbMock.on(UpdateCommand).resolves({ Attributes: item });
  });
  it('updates item with changes and parameters', async () => {
    await instance.updateV2(item, { x: 1 }, { opts: { ConditionExpression: 'conditionExpression' } });

    expect(ddbMock).toHaveReceivedCommandWith(UpdateCommand, {
      ConditionExpression: 'conditionExpression AND attribute_not_exists(version)',
      Key: item,
      UpdateExpression: expect.stringContaining('#k_x = :v_x'),
      ExpressionAttributeNames: expect.objectContaining({ '#k_x': 'x' }),
      ExpressionAttributeValues: expect.objectContaining({ ':v_x': 1 })
    });
  });
  it('returns the object (defined by ReturnValues)', async () => {
    const res = await instance.updateV2(item, { x: 1 }, { opts: { ReturnValues: 'ALL_NEW' } });

    expect(res).toEqual(item);
  });
  it('throws on error', () => {
    ddbMock.on(UpdateCommand).rejects(new Error('err'));

    const throws = async () => {
      await instance.updateV2(item);
    };

    expect(throws).rejects.toThrow('err');
  });
});

describe('queryIndex', () => {
  const items = [{}];
  beforeEach(() => {
    ddbMock.on(QueryCommand).resolves({ Items: items });
  });
  it('rejects promise when index is not defined', async () => {
    async function throws() {
      await instance.queryIndex('some-index', {});
    }

    await expect(throws).rejects.toThrowError('index "some-index" is not defined in the model');
  });
  it('queries the correct index', async () => {
    await instance.queryIndex('createdBy-dateCreated-index', { createdBy: 'a' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, { TableName: 'stg-test', IndexName: 'createdBy-dateCreated-index' });
  });
  it('queries using hashKey when only hashKey is provided', async () => {
    const value = 2121345;
    await instance.queryIndex('dateCreated-index', { dateCreated: value });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      TableName: 'stg-test',
      IndexName: 'dateCreated-index',
      ExpressionAttributeNames: { '#hkn': 'dateCreated' },
      KeyConditionExpression: '#hkn = :hkv',
      ExpressionAttributeValues: {
        ':hkv': value
      }
    });
  });
  it('queries using hashKey and rangeKey when both are provided', async () => {
    const dateCreated = 2121345,
      createdBy = 'test';

    await instance.queryIndex('createdBy-dateCreated-index', { createdBy, dateCreated });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      TableName: 'stg-test',
      IndexName: 'createdBy-dateCreated-index',
      ExpressionAttributeNames: { '#hkn': 'createdBy', '#rkn': 'dateCreated' },
      KeyConditionExpression: '#hkn = :hkv AND #rkn = :rkv',
      ExpressionAttributeValues: {
        ':hkv': createdBy,
        ':rkv': dateCreated
      }
    });
  });
  it('retrieves paginated results', async () => {
    const allItems = setupPaginatedResults(2);
    const res = await instance.queryIndex('dateCreated-index', { dateCreated: 302487 });
    expect(res).toEqual({ items: allItems, lastEvaluatedKey: undefined });
  });
  it('truncates pagination results after 7 requests', async () => {
    setupPaginatedResults(8);

    const res = await instance.queryIndex('dateCreated-index', { dateCreated: 302487 });

    expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 7);
    expect(res.items.length).toEqual(70);
  });
  it('rejects request if an error occurs', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({ Items: [{ item: true }], LastEvaluatedKey: { item: true } })
      .rejectsOnce(new Error('err'));

    await expect(async () => await instance.queryIndex('dateCreated-index', { dateCreated: 302487 })).rejects.toThrow('err');
  });
  it('queries using specified rangeOp', async () => {
    await instance.queryIndex('createdBy-dateCreated-index', { createdBy: 'a', dateCreated: 101 }, { rangeOp: '>' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv AND #rkn > :rkv'
    });
  });
  it('queries using begins_with rangeOp', async () => {
    await instance.queryIndex('createdBy-dateCreated-index', { createdBy: 'a', dateCreated: 101 }, { rangeOp: 'begins_with' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv AND begins_with(#rkn, :rkv)'
    });
  });
});

describe('query', () => {
  const items = [{}];
  beforeEach(() => {
    ddbMock.on(QueryCommand).resolves({ Items: items });
  });

  it('queries the correct table', async () => {
    await instance.query({ id: 'a' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, { TableName: 'stg-test' });
  });
  it('rejects promise when the hashKey was not found on the provided object', async () => {
    async function throws() {
      await instance.query({});
    }

    await expect(throws).rejects.toThrowError('hashKey id was not found on keyObj');
  });
  it('queries using hashKey when only hashKey is provided', async () => {
    await instance.query({ id: 'a' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv',
      ExpressionAttributeNames: { '#hkn': 'id' },
      ExpressionAttributeValues: { ':hkv': 'a' }
    });
  });
  it('queries using hashKey and rangeKey when both are provided', async () => {
    await instance.query({ id: 'a', range: 'b' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv AND #rkn = :rkv',
      ExpressionAttributeNames: { '#hkn': 'id', '#rkn': 'range' },
      ExpressionAttributeValues: { ':hkv': 'a', ':rkv': 'b' }
    });
  });
  it('retrieves paginated results', async () => {
    const allItems = setupPaginatedResults(2);

    const res = await instance.query({ id: 'a', range: 'b' });
    expect(res.items).toEqual(allItems);
  });
  it('truncates pagination results after 7 requests', async () => {
    setupPaginatedResults(8);

    const res = await instance.query({ id: 'a', range: 'b' });

    expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 7);
    expect(res.items.length).toEqual(70);
  });
  it('rejects request if an error occurs', async () => {
    ddbMock
      .on(QueryCommand)
      .resolvesOnce({ Items: [{ item: true }], LastEvaluatedKey: { item: true } })
      .rejectsOnce(new Error('err'));

    await expect(async () => await instance.query({ id: 'a' })).rejects.toThrow('err');
  });
  it('queries using specified rangeOp', async () => {
    await instance.query({ id: 'a', range: 'b' }, { rangeOp: '>' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv AND #rkn > :rkv'
    });
  });
  it('queries using begins_with rangeOp', async () => {
    await instance.query({ id: 'a', range: 'b' }, { rangeOp: 'begins_with' });

    expect(ddbMock).toHaveReceivedCommandWith(QueryCommand, {
      KeyConditionExpression: '#hkn = :hkv AND begins_with(#rkn, :rkv)'
    });
  });
});

describe('all', () => {
  beforeEach(() => {
    ddbMock.on(ScanCommand).resolves({ Items: [{ id: 'a' }] });
  });
  it('queries the correct table', async () => {
    await instance.all();

    expect(ddbMock).toHaveReceivedCommandWith(ScanCommand, { TableName: 'stg-test' });
  });
  it('retrieves paginated results', async () => {
    ddbMock
      .on(ScanCommand)
      .resolvesOnce({ Items: [{ id: 'a' }], LastEvaluatedKey: { id: 'a' } })
      .resolvesOnce({ Items: [{ id: 'b' }] });

    const res = await instance.all();

    expect(res).toEqual([{ id: 'a' }, { id: 'b' }]);
  });
  it('truncates pagination results after 7 requests', async () => {
    ddbMock.on(ScanCommand).resolves({ Items: [{ id: 'a' }], LastEvaluatedKey: { id: 'a' } });
    const res = await instance.all();

    expect(res?.length).toEqual(7);
  });
  it('rejects request if an error occurs', async () => {
    ddbMock.on(ScanCommand).rejects(new Error('err'));

    const throws = async () => {
      await instance.all();
    };

    await expect(throws).rejects.toThrow('err');
  });
});

//TODO vanilla update methods
//TODO Support tx calls - executetransactioncommand
//TODO PartiQL - executestatementcommand
//Todo refactor options - move prefix to opts
//todo doc breaking changes?
//todo async lib for batching?, configurable, overrideable batch size
//todo updateV2 migration?
//TODO CI PIPELINE, testing, releasing
