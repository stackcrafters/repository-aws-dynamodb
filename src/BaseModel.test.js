import BaseModel from './BaseModel';
// import { v4 as uuidv4 } from 'uuid';
import dynamodb from './utils/dynamodb';

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

class TestModel extends BaseModel {
  constructor() {
    super(config);
  }
}

const instance = new TestModel();
Object.freeze(instance);

class TestModelPostfix extends BaseModel {
  constructor() {
    super({...config, stage: 'postfix'});
  }
}

const postfixInstance = new TestModelPostfix();
Object.freeze(postfixInstance);

jest.mock('./utils/dynamodb');

beforeEach(() => {
  jest.resetModules();
  jest.resetAllMocks();
});

const setupPaginatedResults = (pageCount = 8) => {
  const pageSize = 10;
  const totalItemCount = pageCount * pageSize;
  let allItems = [];
  for (let i = 0; i < totalItemCount; i += pageSize) {
    const batch = [...Array(pageSize).keys()].map((n) => ({ id: `k${i + n}`, range: `r${i + n}` }));
    dynamodb.query.mockImplementationOnce((_, cb) =>
      cb(false, { Items: batch, ...(i + pageSize < totalItemCount ? { LastEvaluatedKey: 'xyz' } : {}) })
    );
    allItems = allItems.concat(batch);
  }
  return allItems;
};

describe('get', () => {
  const item = {};
  beforeEach(() => {
    dynamodb.get.mockReturnValue({ promise: () => Promise.resolve({ Item: item }) });
  });

  it('queries the correct table name (prefix)', async () => {
    await instance.get({ id: 'id1', range: 'r1' });
    expect(dynamodb.get).toHaveBeenCalledWith(expect.objectContaining({ TableName: 'stg-test' }));
  });
  it('queries the correct table name (postfix)', async () => {
    await postfixInstance.get({ id: 'id1', range: 'r1' });
    expect(dynamodb.get).toHaveBeenCalledWith(expect.objectContaining({ TableName: 'test-stg' }));
  });
  it('queries using the object hash and range keys', async () => {
    await instance.get({ id: 'k1', range: 'r1', unrelated: 'pickles' });
    expect(dynamodb.get).toHaveBeenCalledWith(
      expect.objectContaining({
        Key: {
          id: 'k1',
          range: 'r1'
        }
      })
    );
  });
  it('returns the item from the response', async () => {
    const res = await instance.get({ id: 'k1', range: 'r1' });
    expect(res).toEqual(item);
  });
  it('returns undefined when the item does not exist', async () => {
    dynamodb.get.mockReturnValue({ promise: () => Promise.reject('err') });

    const result = await instance.get({ id: 'k1', range: 'r1' });

    expect(result).toBeUndefined();
  });
});

describe('remove', () => {
  beforeEach(() => {
    dynamodb.delete.mockReturnValue({ promise: () => Promise.resolve() });
  });
  it('queries the correct table name', async () => {
    await instance.remove({ id: 'k1', range: 'r1' });
    expect(dynamodb.delete).toHaveBeenCalledWith(expect.objectContaining({ TableName: 'stg-test' }));
  });
  it('queries using the object hash and range keys', async () => {
    await instance.remove({ id: 'k1', range: 'r1', unrelated: 'pickles' });
    expect(dynamodb.delete).toHaveBeenCalledWith(
      expect.objectContaining({
        Key: {
          id: 'k1',
          range: 'r1'
        }
      })
    );
  });
});

describe('removeBatch', () => {
  beforeEach(() => {
    dynamodb.batchWrite.mockReturnValue({ promise: () => Promise.resolve({}) });
  });
  it('queries the correct table name', async () => {
    await instance.removeBatch([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);
    expect(dynamodb.batchWrite).toHaveBeenCalledWith({ RequestItems: { 'stg-test': expect.anything() } });
  });
  it('batches requests into batches of 25', async () => {
    const objs = [...Array(30).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    await instance.removeBatch(objs);

    expect(dynamodb.batchWrite).toHaveBeenCalledTimes(2);
    expect(dynamodb.batchWrite.mock.calls[0][0].RequestItems['stg-test'].length).toBe(25);
    expect(dynamodb.batchWrite.mock.calls[1][0].RequestItems['stg-test'].length).toBe(5);
  });
  it('queries using the object hash and range keys', async () => {
    await instance.removeBatch([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);

    expect(dynamodb.batchWrite.mock.calls[0][0].RequestItems['stg-test']).toEqual([
      { DeleteRequest: { Key: { id: 'k1', range: 'r1' } } },
      { DeleteRequest: { Key: { id: 'k2', range: 'r2' } } }
    ]);
  });
  // it('throws on promise rejection', async () => {
  //   dynamodb.batchWrite.mockReturnValue({ promise: () => Promise.reject('err') });
  //   async function throws() {
  //     let newVar = await instance.removeBatch({ id: 'k1', range: 'r1' });
  //     console.log('completed')
  //   }
  //
  //   await expect(throws()).rejects.toThrowError('err');
  // });
  it('returns UnprocessedItems', async () => {
    let items = [1234];
    dynamodb.batchWrite.mockReturnValue({ promise: () => Promise.resolve({ UnprocessedItems: { 'stg-test': items } }) });

    const res = await instance.removeBatch([{ id: 'k1', range: 'r1' }]);

    expect(res).toEqual(items);
  });
});

describe('getBatch', () => {
  const items = [{}];
  beforeEach(() => {
    dynamodb.batchGet.mockReturnValue({ promise: () => Promise.resolve({ Responses: { 'stg-test': items } }) });
  });
  it('queries the correct table name', async () => {
    await instance.getBatch([{ id: 'k1', range: 'r1' }]);

    expect(dynamodb.batchGet).toHaveBeenCalledWith({ RequestItems: { 'stg-test': expect.anything() } });
  });
  it('batches requests into batches of 100', async () => {
    const objs = [...Array(110).keys()].map((n) => ({ id: `k${n}`, range: `r${n}` }));

    await instance.getBatch(objs);

    expect(dynamodb.batchGet).toHaveBeenCalledTimes(2);
    expect(dynamodb.batchGet.mock.calls[0][0].RequestItems['stg-test'].Keys.length).toBe(100);
    expect(dynamodb.batchGet.mock.calls[1][0].RequestItems['stg-test'].Keys.length).toBe(10);
  });
  it('queries using object hash and range keys', async () => {
    await instance.getBatch([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);

    expect(dynamodb.batchGet.mock.calls[0][0].RequestItems['stg-test'].Keys).toEqual([
      { id: 'k1', range: 'r1' },
      { id: 'k2', range: 'r2' }
    ]);
  });
  // it('throws on promise rejection', async () => {
  //   dynamodb.batchGet.mockReturnValue({ promise: () => Promise.reject('err') });
  //   async function throws() {
  //     await instance.getBatch({ id: 'k1', range: 'r1' });
  //   }
  //
  //   await expect(throws()).rejects.toThrowError('err');
  // });
});

describe('queryIndex', () => {
  const items = [{}];
  beforeEach(() => {
    dynamodb.query.mockImplementation((_, cb) => cb(false, { Items: items }));
  });

  it('rejects promise when index is not defined', async () => {
    async function throws() {
      await instance.queryIndex('some-index', {});
    }

    await expect(throws()).rejects.toThrowError('index "some-index" is not defined in the model');
  });

  it('queries the correct index', async () => {
    await instance.queryIndex('createdBy-dateCreated-index', { createdBy: 'a' });

    expect(dynamodb.query).toHaveBeenCalledWith(
      expect.objectContaining({ TableName: 'stg-test', IndexName: 'createdBy-dateCreated-index' }),
      expect.anything()
    );
  });

  it('queries using hashKey when only hashKey is provided', async () => {
    const value = 2121345;
    await instance.queryIndex('dateCreated-index', { dateCreated: value });

    expect(dynamodb.query).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'stg-test',
        IndexName: 'dateCreated-index',
        ExpressionAttributeNames: { '#hkn': 'dateCreated' },
        KeyConditionExpression: '#hkn = :hkv',
        ExpressionAttributeValues: {
          ':hkv': value
        }
      }),
      expect.anything()
    );
  });

  it('queries using hashKey and rangeKey when both are provided', async () => {
    const dateCreated = 2121345,
      createdBy = 'test';

    await instance.queryIndex('createdBy-dateCreated-index', { createdBy, dateCreated });

    expect(dynamodb.query).toHaveBeenCalledWith(
      expect.objectContaining({
        TableName: 'stg-test',
        IndexName: 'createdBy-dateCreated-index',
        ExpressionAttributeNames: { '#hkn': 'createdBy', '#rkn': 'dateCreated' },
        KeyConditionExpression: '#hkn = :hkv AND #rkn = :rkv',
        ExpressionAttributeValues: {
          ':hkv': createdBy,
          ':rkv': dateCreated
        }
      }),
      expect.anything()
    );
  });

  it('retrieves paginated results', async () => {
    const allItems = setupPaginatedResults(2);
    const res = await instance.queryIndex('dateCreated-index', { dateCreated: 302487 });
    expect(res).toEqual({ items: allItems });
  });

  it('truncates pagination results after 7 requests', async () => {
    setupPaginatedResults(8);

    const res = await instance.queryIndex('dateCreated-index', { dateCreated: 302487 });

    expect(dynamodb.query).toHaveBeenCalledTimes(7);
    expect(res.items.length).toEqual(70);
  });

  it('rejects request if an error occurs', async () => {
    dynamodb.query.mockImplementationOnce((_, cb) => cb(false, { Items: [{ item: true }], LastEvaluatedKey: 'xyz' }));
    dynamodb.query.mockImplementationOnce((_, cb) => cb(true));

    const throws = async () => {
      await instance.queryIndex('dateCreated-index', { dateCreated: 302487 });
    };

    expect(throws()).rejects.toThrow();
  });
});

describe('query', () => {
  const items = [{}];
  beforeEach(() => {
    dynamodb.query.mockImplementation((_, cb) => cb(false, { Items: items }));
  });

  it('queries the correct table', async () => {
    await instance.query({ id: 'a' });

    expect(dynamodb.query).toHaveBeenCalledWith(expect.objectContaining({ TableName: 'stg-test' }), expect.anything());
  });
  it('rejects promise when the hashKey was not found on the provided object', async () => {
    async function throws() {
      await instance.query({});
    }

    await expect(throws()).rejects.toThrowError('hashKey id was not found on keyObj');
  });
  it('queries using hashKey when only hashKey is provided', async () => {
    await instance.query({ id: 'a' });

    expect(dynamodb.query).toHaveBeenCalledWith(
      expect.objectContaining({
        KeyConditionExpression: '#hkn = :hkv',
        ExpressionAttributeNames: { '#hkn': 'id' },
        ExpressionAttributeValues: { ':hkv': 'a' }
      }),
      expect.anything()
    );
  });
  it('queries using hashKey and rangeKey when both are provided', async () => {
    await instance.query({ id: 'a', range: 'b' });

    expect(dynamodb.query).toHaveBeenCalledWith(
      expect.objectContaining({
        KeyConditionExpression: '#hkn = :hkv AND #rkn = :rkv',
        ExpressionAttributeNames: { '#hkn': 'id', '#rkn': 'range' },
        ExpressionAttributeValues: { ':hkv': 'a', ':rkv': 'b' }
      }),
      expect.anything()
    );
  });
  it('retrieves paginated results', async () => {
    const allItems = setupPaginatedResults(2);

    const res = await instance.query({ id: 'a', range: 'b' });
    expect(res.items).toEqual(allItems);
  });
  it('truncates pagination results after 7 requests', async () => {
    setupPaginatedResults(8);

    const res = await instance.query({ id: 'a', range: 'b' });

    expect(dynamodb.query).toHaveBeenCalledTimes(7);
    expect(res.items.length).toEqual(70);
  });
  // it('rejects request if an error occurs');
  //rangeop begins with
});

describe('all', () => {});

describe('save', () => {});

describe('saveBatch', () => {});
