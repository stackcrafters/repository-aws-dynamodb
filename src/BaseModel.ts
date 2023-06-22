import { chunkArray } from './utils/array';
import { dateNow } from './utils/date';
import { dbClient } from './utils/dynamoDbv3';
import {
  BatchGetCommand,
  BatchWriteCommand,
  DeleteCommand,
  GetCommand,
  paginateQuery,
  paginateScan,
  PutCommand,
  PutCommandInput,
  UpdateCommand,
  UpdateCommandInput
} from '@aws-sdk/lib-dynamodb';
import { Paginator } from '@aws-sdk/types';

const getVersionCondition = (item: BaseObject): string => {
  if (item.version) {
    return 'version = :ver';
  }
  return 'attribute_not_exists(version)';
};

const getVersionValues = (item: BaseObject): { [key: string]: number } | undefined => {
  if (item.version) {
    return { ':ver': item.version };
  }
};

export interface BaseObject {
  dateUpdated?: number;
  dateCreated?: number;
  version?: number;
  updatedBy?: string;
}

interface Keys {
  hashKey: string;
  rangeKey?: string;
  globalIndexes?: { [key: string]: Keys };
}

type GetOpts = {
  ExpressionAttributeNames?: Record<string, string>;
  ProjectionExpression?: string;
};

type QueryOpts = GetOpts & {
  ScanIndexForward?: boolean;
  Limit?: number;
  ExpressionAttributeValues?: Record<string, any>;
  KeyConditionExpression?: string;
  ExclusiveStartKey?: any;
};

type Config = {
  dateUnits: 's' | 'ms';
};

type UpdateOpts = {
  opts?: {
    ExpressionAttributeNames?: Record<string, string>;
    ExpressionAttributeValues?: Record<string, any>;
    ConditionExpression?: string;
    UpdateExpression?: string;
    ReturnValues?: string;
  };
  skipVersionCondition?: boolean;
};

export default class BaseModel<T extends BaseObject> {
  private readonly tableName: string;
  private readonly keys: Keys;
  private readonly config: Config;

  constructor({
    tableName,
    stage = 'prefix',
    keys,
    config
  }: {
    tableName: string;
    stage?: 'postfix' | 'prefix' | 'none';
    keys: Keys;
    config?: Config;
  }) {
    if (stage === 'none') {
      this.tableName = tableName;
    } else {
      this.tableName = stage === 'prefix' ? `${process.env.SERVERLESS_STAGE}-${tableName}` : `${tableName}-${process.env.SERVERLESS_STAGE}`;
    }
    this.keys = keys;
    this.config = {
      dateUnits: 's',
      ...(config || {})
    };
  }

  currentTimestamp = () => {
    if (this.config.dateUnits === 's') {
      return dateNow(); //s
    }
    return Date.now(); //ms
  };

  baseAttributes = (): BaseObject => ({
    dateUpdated: this.currentTimestamp(),
    dateCreated: this.currentTimestamp()
  });

  createKey = (item: BaseObject, keySpec = this.keys): Record<string, any> => ({
    [keySpec.hashKey]: item[keySpec.hashKey],
    ...(keySpec.rangeKey && item.hasOwnProperty(keySpec.rangeKey) ? { [keySpec.rangeKey]: item[keySpec.rangeKey] } : {})
  });

  createIndexKey = (index: string, item: BaseObject) => ({
    ...this.createKey(item),
    ...this.createKey(item, this.keys.globalIndexes?.[index])
  });

  private validateItemKeys(item, action = 'saved') {
    if (!item[this.keys.hashKey]) {
      throw new Error(`item being ${action} does not contain a valid hashKey (${this.keys.hashKey})`);
    }
    if (this.keys.rangeKey && !item[this.keys.rangeKey]) {
      throw new Error(`item being ${action} does not contain a valid rangeKey (${this.keys.rangeKey})`);
    }
  }

  private getPaginatedResult = async (paginator: Paginator<any>, maxRequests = 7, indexName?: string) => {
    let items: T[] = [];
    let request = 0;
    let done = false;
    while (maxRequests > request && !done) {
      const { value: itemData, done: paginatorDone } = await paginator.next();
      done = paginatorDone ?? true;
      if (itemData) {
        items = items.concat(itemData.Items);
      }
      request++;
    }
    if (maxRequests <= request) {
      console.warn(`dynamodb results truncated, maximum number of requests was reached (${maxRequests})`);
    }
    let lastEvaluatedKey: any = undefined;
    if (!done && items.length > 0) {
      const lastItem = items[items.length - 1];
      lastEvaluatedKey = indexName ? this.createIndexKey(indexName, lastItem) : this.createKey(lastItem);
    }
    return { items, lastEvaluatedKey };
  };

  get = async (keyObj, { consistentRead = true, opts = {} }: { opts?: GetOpts; consistentRead?: boolean } = {}): Promise<T | undefined> => {
    const key = this.createKey(keyObj);
    try {
      const { Item } = await dbClient.send(
        new GetCommand({
          TableName: this.tableName,
          Key: key,
          ConsistentRead: consistentRead,
          ...opts
        })
      );
      return <T>Item;
    } catch (err) {
      console.warn(`Object with key ${JSON.stringify(key)} not found`, err);
      return undefined;
    }
  };

  remove = async (keyObj) => {
    await dbClient.send(
      new DeleteCommand({
        TableName: this.tableName,
        Key: this.createKey(keyObj)
      })
    );
  };

  removeBatch = async (keyObjs: any[]) => {
    try {
      const results = await Promise.all(
        chunkArray(keyObjs, 25).map((objs) =>
          dbClient.send(
            new BatchWriteCommand({
              RequestItems: {
                [this.tableName]: objs.map((o) => {
                  return { DeleteRequest: { Key: this.createKey(o) } };
                })
              }
            })
          )
        )
      );
      //return unprocessed items
      return results.reduce<any[]>((acc, data) => {
        if (data?.UnprocessedItems?.[this.tableName]) {
          return acc.concat(data.UnprocessedItems[this.tableName]);
        }
        return acc;
      }, []);
    } catch (err) {
      console.error('error performing removeBatch', err);
      throw err;
    }
  };

  getBatch = async (keyObjs: any[], consistentRead = true, { opts = {} } = {}): Promise<T[]> => {
    const results = await Promise.all(
      chunkArray(keyObjs, 100).map((pairs) =>
        dbClient.send(
          new BatchGetCommand({
            RequestItems: {
              [this.tableName]: {
                Keys: pairs.map((p) => this.createKey(p)),
                ConsistentRead: consistentRead,
                ...opts
              }
            }
          })
        )
      )
    );
    return results.reduce<T[]>((acc, result) => {
      if (result.Responses?.[this.tableName]) {
        return acc.concat(<T[]>result.Responses[this.tableName]);
      }
      return acc;
    }, []);
  };

  queryIndex = async (
    index: string,
    keyObj,
    {
      opts = { ExpressionAttributeNames: {}, ExpressionAttributeValues: {} },
      rangeOp = '=',
      maxRequests = 7
    }: { opts?: QueryOpts; rangeOp?: string; pageSize?: number; maxRequests?: number } = {}
  ): Promise<{ items: T[]; lastEvaluatedKey?: Partial<T> }> => {
    const ind = this.keys.globalIndexes?.[index];
    if (!ind || !ind.hashKey) {
      throw new Error(`index "${index}" is not defined in the model`);
    }
    const { hashKey, rangeKey } = ind;
    const rangeKeyPresent = rangeKey && keyObj[rangeKey];

    const { ExpressionAttributeNames = {}, ExpressionAttributeValues = {}, ...otherOpts } = opts;

    return await this.getPaginatedResult(
      paginateQuery(
        { client: dbClient, pageSize: opts?.Limit, startingToken: opts?.ExclusiveStartKey },
        {
          TableName: this.tableName,
          IndexName: index,
          KeyConditionExpression: `#hkn = :hkv${
            rangeKeyPresent ? ` AND ${rangeOp === 'begins_with' ? `begins_with(#rkn, :rkv)` : `#rkn ${rangeOp} :rkv`}` : ''
          }`,
          ExpressionAttributeNames: {
            ...ExpressionAttributeNames,
            '#hkn': hashKey,
            ...(rangeKeyPresent ? { '#rkn': rangeKey } : {})
          },
          ExpressionAttributeValues: {
            ...ExpressionAttributeValues,
            ':hkv': keyObj[hashKey],
            ...(rangeKeyPresent ? { ':rkv': keyObj[rangeKey] } : {})
          },
          ...otherOpts
        }
      ),
      maxRequests,
      index
    );
  };

  query = async (
    keyObj,
    {
      opts = { ExpressionAttributeNames: {}, ExpressionAttributeValues: {} },
      rangeOp = '=',
      consistentRead = true,
      maxRequests = 7
    }: { opts?: QueryOpts; rangeOp?: string; consistentRead?: boolean; pageSize?: number; maxRequests?: number } = {}
  ): Promise<{ items: T[]; lastEvaluatedKey?: Partial<T> }> => {
    const hashKey = <string>this.keys.hashKey;
    if (!keyObj[hashKey]) {
      throw new Error(`hashKey ${hashKey} was not found on keyObj`);
    }
    const rangeKey = <string>this.keys.rangeKey;
    const rangeKeyPresent = rangeKey && keyObj[rangeKey];

    const { ExpressionAttributeNames = {}, ExpressionAttributeValues = {}, ...otherOpts } = opts;

    return await this.getPaginatedResult(
      paginateQuery(
        { client: dbClient, pageSize: opts?.Limit, startingToken: opts?.ExclusiveStartKey },
        {
          TableName: this.tableName,
          KeyConditionExpression: `#hkn = :hkv${
            rangeKeyPresent ? ` AND ${rangeOp === 'begins_with' ? `begins_with(#rkn, :rkv)` : `#rkn ${rangeOp} :rkv`}` : ''
          }`,
          ExpressionAttributeNames: {
            ...ExpressionAttributeNames,
            '#hkn': hashKey,
            ...(rangeKeyPresent ? { '#rkn': rangeKey } : {})
          },
          ExpressionAttributeValues: {
            ...ExpressionAttributeValues,
            ':hkv': keyObj[hashKey],
            ...(rangeKeyPresent ? { ':rkv': keyObj[rangeKey] } : {})
          },
          ConsistentRead: consistentRead,
          ...otherOpts
        }
      ),
      maxRequests
    );
  };

  all = async (): Promise<T[] | undefined> => {
    const result = await this.getPaginatedResult(paginateScan({ client: dbClient }, { TableName: this.tableName }));
    return result.items;
  };

  prepareSave = (item: T, userId?, conditionExpression?: string): PutCommandInput => {
    this.validateItemKeys(item);

    const versionCondition = getVersionCondition(item);
    const versionValues = getVersionValues(item);
    item.version = item.version ? item.version + 1 : 1;

    if (item.dateUpdated) {
      item.dateUpdated = this.currentTimestamp();
    }
    if (item.updatedBy && userId) {
      item.updatedBy = userId;
    }

    return {
      TableName: this.tableName,
      Item: <T & { [p: string]: any }>item,
      ReturnValues: 'ALL_OLD',
      ConditionExpression: conditionExpression ? `${conditionExpression} AND ${versionCondition}` : versionCondition,
      ...(versionValues ? { ExpressionAttributeValues: versionValues } : {})
    };
  };

  save = async (item: T, userId?, conditionExpression?: string): Promise<T> => {
    const params = this.prepareSave(item, userId, conditionExpression);
    return <T>(await dbClient.send(new PutCommand(params))).Attributes;
  };

  saveBatch = async (items: any[], userId?) => {
    return await Promise.all(
      chunkArray(items, 25).map((itemBatch) =>
        dbClient.send(
          new BatchWriteCommand({
            RequestItems: {
              [this.tableName]: itemBatch.map((item) => {
                this.validateItemKeys(item);
                if (item.dateUpdated) {
                  item.dateUpdated = this.currentTimestamp();
                }
                if (item.updatedBy && userId) {
                  item.updatedBy = userId;
                }
                return {
                  PutRequest: {
                    Item: item
                  }
                };
              })
            }
          })
        )
      )
    );
  };

  prepareUpdate = (keyObj, opts = {}): UpdateCommandInput => {
    const key = this.createKey(keyObj);
    return {
      TableName: this.tableName,
      Key: key,
      ...opts
    };
  };

  update = async (keyObj, opts = {}): Promise<T | undefined> => {
    const data = await dbClient.send(new UpdateCommand(this.prepareUpdate(keyObj, opts)));
    return <T>data.Attributes;
  };

  prepareUpdateV2 = (
    item,
    changes = {},
    { skipVersionCondition = false, opts: overrideOpts = {} }: UpdateOpts = { opts: {} },
    userId?: string
  ): UpdateCommandInput => {
    const opts = {
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {},
      ConditionExpression: undefined,
      UpdateExpression: undefined,
      ReturnValues: 'ALL_NEW',
      ...overrideOpts
    };
    this.validateItemKeys(item, 'updated');

    const names = {
      '#version': 'version',
      '#dateCreated': 'dateCreated',
      '#dateUpdated': 'dateUpdated',
      ...(userId ? { '#createdBy': 'createdBy', '#updatedBy': 'updatedBy' } : {})
    };
    const values = {
      ...(skipVersionCondition ? {} : { ...getVersionValues(item) }),
      ...(userId ? { ':userId': userId } : {}),
      ':v_0': 0,
      ':v_1': 1,
      ':now': this.currentTimestamp()
    };
    Object.entries(changes).forEach(([k, v]) => {
      names[`#k_${k}`] = k;
      values[`:v_${k}`] = v;
    });
    return {
      TableName: this.tableName,
      Key: this.createKey(item),
      ExpressionAttributeNames: { ...names, ...opts?.ExpressionAttributeNames },
      ExpressionAttributeValues: { ...values, ...opts?.ExpressionAttributeValues },
      ConditionExpression: skipVersionCondition
        ? opts?.ConditionExpression
        : `${opts?.ConditionExpression ? `${opts.ConditionExpression} AND ` : ''}${getVersionCondition(item)}`,
      ReturnValues: opts?.ReturnValues,
      UpdateExpression:
        (opts?.UpdateExpression
          ? `${opts?.UpdateExpression}, ${opts.UpdateExpression?.indexOf('SET') > -1 ? '' : 'SET '}`
          : `SET ${Object.keys(changes)
              .map((k) => `#k_${k} = :v_${k}`)
              .join(', ')}, `) +
        '#version = if_not_exists(#version, :v_0) + :v_1, ' +
        (userId ? '#createdBy = if_not_exists(#createdBy, :userId), #updatedBy = :userId, ' : '') +
        '#dateCreated = if_not_exists(#dateCreated, :now), ' +
        '#dateUpdated = :now'
    };
  };

  updateV2 = async (item, changes = {}, updateOpts?: UpdateOpts, userId?: string): Promise<T | undefined> => {
    const result = await dbClient.send(new UpdateCommand(this.prepareUpdateV2(item, changes, updateOpts, userId)));
    return <T>result?.Attributes;
  };
}
