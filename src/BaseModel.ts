import dynamoDb from './utils/dynamodb';
import { chunkArray } from './utils/array';
import { dateNow } from './utils/date';
import { AWSError } from 'aws-sdk';

const createKey = (keys: Keys, item: BaseObject): Record<string, string> => ({
  [keys.hashKey]: item[keys.hashKey],
  ...(keys.rangeKey && item[keys.rangeKey] ? { [keys.rangeKey]: item[keys.rangeKey] } : {})
});

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

type Opts = {
  ScanIndexForward?: boolean;
  Limit?: number;
  ExpressionAttributeNames?: Record<string, string>;
  ExpressionAttributeValues?: Record<string, any>;
  KeyConditionExpression?: string;
};

type Config = {
    dateUnits: 's' | 'ms';
}

export default class BaseModel<T extends BaseObject> {
  private readonly tableName: string;
  private readonly keys: Keys;
  private readonly config: Config;

  constructor({ tableName, stage = 'prefix', keys, config }: { tableName: string; stage?: 'postfix' | 'prefix' | 'none'; keys: Keys; config?: Config }) {
    if (stage === 'none') {
      this.tableName = tableName;
    } else {
      this.tableName = stage === 'prefix' ? `${process.env.SERVERLESS_STAGE}-${tableName}` : `${tableName}-${process.env.SERVERLESS_STAGE}`;
    }
    this.keys = keys;
    this.config = {
        dateUnits: 's',
        ...(config || {})
    }
  }

  currentTimestamp = () => {
      if(this.config.dateUnits === 's'){
          return dateNow();//s
      }
      return Date.now();//ms
  }

  baseAttributes = (): BaseObject => ({
    dateUpdated: this.currentTimestamp(),
    dateCreated: this.currentTimestamp(),
    version: 1
  });

  getPaginatedResult = (
    fn,
    params,
    cb: (err?: AWSError | boolean, items?: T[], lastEvaluatedKey?: string) => void,
    maxRequests = 7,
    request = 1,
    items = <T[]>[]
  ): void => {
    fn.call(dynamoDb, params, (err, data) => {
      if (err) {
        cb(err, items);
      } else {
        if (data.LastEvaluatedKey && maxRequests > request) {
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          this.getPaginatedResult(fn, params, cb, maxRequests, request + 1, items.concat(data.Items || []));
        } else {
          if (maxRequests <= request) {
            console.warn(`dynamodb results truncated, maximum number of requests was reached (${maxRequests})`);
          }
          cb(false, items.concat(data.Items || []), data.LastEvaluatedKey);
        }
      }
    });
  };

  get = (keyObj, consistentRead = true): Promise<T | undefined> => {
    const key = createKey(this.keys, keyObj);
    return <Promise<T>>dynamoDb
      .get({
        TableName: this.tableName,
        Key: key,
        ConsistentRead: consistentRead
      })
      .promise()
      .then(
        (data) => {
          return data.Item;
        },
        (err) => {
          console.warn(`Object with key ${JSON.stringify(key)} not found`, err);
          return undefined;
        }
      );
  };

  remove = (keyObj) => {
    return dynamoDb
      .delete({
        TableName: this.tableName,
        Key: createKey(this.keys, keyObj)
      })
      .promise();
  };

  removeBatch = (keyObjs: any[]) =>
    Promise.all(
      chunkArray(keyObjs, 25).map((objs) =>
        dynamoDb
          .batchWrite({
            RequestItems: {
              [this.tableName]: objs.map((o) => {
                return { DeleteRequest: { Key: createKey(this.keys, o) } };
              })
            }
          })
          .promise()
          .then((data) => {
            if (data.UnprocessedItems && data.UnprocessedItems[this.tableName]) {
              return data.UnprocessedItems[this.tableName];
            } else {
              return [];
            }
          })
          .catch((err) => {
            console.error('error performing removeBatch', err);
            throw err;
          })
      )
    ).then((arr) => arr.flat());

  getBatch = (keyObjs: any[], consistentRead = true, { opts = {} } = {}): Promise<T[]> => <Promise<T[]>>Promise.all(
      chunkArray(keyObjs, 100).map((pairs) =>
        dynamoDb
          .batchGet({
            RequestItems: {
              [this.tableName]: {
                Keys: pairs.map((p) => createKey(this.keys, p)),
                ConsistentRead: consistentRead,
                ...opts
              }
            }
          })
          .promise()
          .then(
            (data) => {
              if (data.Responses && data.Responses[this.tableName]) {
                return data.Responses[this.tableName];
              }
            },
            (err) => {
              throw err;
            }
          )
      )
    ).then((arr) => arr.flat());

  queryIndex = (
    index: string,
    keyObj,
    {
      opts = { ExpressionAttributeNames: {}, ExpressionAttributeValues: {} },
      rangeOp = '=',
      maxRequests = 7
    }: { opts?: Opts; rangeOp?: string; maxRequests?: number } = {}
  ): Promise<{ items: T[]; lastEvaluatedKey?: string }> =>
    new Promise((resolve, reject) => {
      const ind = this.keys.globalIndexes?.[index];
      if (!ind || !ind.hashKey) {
        reject(new Error(`index "${index}" is not defined in the model`));
        return;
      }
      const hashKey = <string>ind.hashKey;
      const rangeKey = <string>ind.rangeKey;
      const rangeKeyPresent = rangeKey && keyObj[rangeKey];

      const { ExpressionAttributeNames = {}, ExpressionAttributeValues = {}, ...otherOpts } = opts;

      this.getPaginatedResult(
        dynamoDb.query,
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
        },
        (err, items, lastEvaluatedKey) => {
          if (err) reject(err);
          else resolve(<{ items: T[]; lastEvaluatedKey?: string }>{ items, lastEvaluatedKey });
        },
        maxRequests
      );
    });

  query = (
    keyObj,
    {
      opts = { ExpressionAttributeNames: {}, ExpressionAttributeValues: {} },
      rangeOp = '=',
      consistentRead = true,
      maxRequests = 7
    }: { opts?: Opts; rangeOp?: string; consistentRead?: boolean; maxRequests?: number } = {}
  ): Promise<{ items: T[]; lastEvaluatedKey?: string }> =>
    new Promise((resolve, reject) => {
      const hashKey = <string>this.keys.hashKey;
      if (!keyObj[hashKey]) {
        reject(new Error(`hashKey ${hashKey} was not found on keyObj`));
        return;
      }
      const rangeKey = <string>this.keys.rangeKey;
      const rangeKeyPresent = rangeKey && keyObj[rangeKey];

      const { ExpressionAttributeNames = {}, ExpressionAttributeValues = {}, ...otherOpts } = opts;

      this.getPaginatedResult(
        dynamoDb.query,
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
        },
        (err, items, lastEvaluatedKey) => {
          if (err) reject(err);
          else resolve(<{ items: T[]; lastEvaluatedKey?: string }>{ items, lastEvaluatedKey });
        },
        maxRequests
      );
    });

  all = (): Promise<T[] | undefined> =>
    new Promise((resolve, reject) => {
      this.getPaginatedResult(dynamoDb.scan, { TableName: this.tableName }, (err, items) => {
        if (err) reject(err);
        else resolve(items);
      });
    });

  save = (item: T, userId?, conditionExpression = false): Promise<T> =>
    new Promise((resolve, reject) => {
      if (!item[this.keys.hashKey]) {
        reject(new Error(`item being saved does not contain a valid hashKey (${this.keys.hashKey}=undefined)`));
        return;
      }
      if (this.keys.rangeKey && !item[this.keys.rangeKey]) {
        reject(new Error(`item being saved does not contain a valid rangeKey (${this.keys.rangeKey}=undefined)`));
        return;
      }

      const versionCondition = getVersionCondition(item);
      const versionValues = getVersionValues(item);
      item.version = item.version ? item.version + 1 : 1;

      if (item.dateUpdated) {
        item.dateUpdated = this.currentTimestamp();
      }
      if (item.updatedBy && userId) {
        item.updatedBy = userId;
      }

      const params = {
        TableName: this.tableName,
        Item: <T & { [p: string]: any }>item,
        ReturnValues: 'ALL_OLD',
        ConditionExpression: conditionExpression ? `${conditionExpression} AND ${versionCondition}` : versionCondition,
        ...(versionValues ? { ExpressionAttributeValues: versionValues } : {})
      };

      dynamoDb.put(params, (error) => {
        if (error) reject(error);
        else resolve(item);
      });
    });

  saveBatch = (items: any[], userId?) =>
    Promise.all(
      chunkArray(items, 25).map((itemBatch) =>
        dynamoDb
          .batchWrite({
            RequestItems: {
              [this.tableName]: itemBatch.map((item) => {
                if (!item[this.keys.hashKey]) {
                  throw new Error(`item being saved does not contain a valid hashKey (${this.keys.hashKey}=undefined)`);
                }
                if (this.keys.rangeKey && !item[this.keys.rangeKey]) {
                  throw new Error(`item being saved does not contain a valid rangeKey (${this.keys.rangeKey}=undefined)`);
                }
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
          .promise()
      )
    );

  update = (keyObj, opts = {}): Promise<T | undefined> => {
    const key = createKey(this.keys, keyObj);
    return <Promise<T>>dynamoDb
      .update({
        TableName: this.tableName,
        Key: key,
        ...opts
      })
      .promise()
      .then(
        (data) => {
          return data.Attributes;
        },
        (err) => {
          throw err;
        }
      );
  };
}
