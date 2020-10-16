import AWS from 'aws-sdk';
import { chunkArray } from './array';

export const dynamodb = new AWS.DynamoDB.DocumentClient({
  maxRetries: 3,
  httpOptions: {
    timeout: 5000
  }
});

const createKey = (keys, { hashKey, rangeKey }) => ({
  [keys.hashKey]: hashKey,
  ...(keys.rangeKey && rangeKey ? { [keys.rangeKey]: rangeKey } : {})
});

const getVersionCondition = (item) => {
  if (item.version) {
    return 'version = :ver';
  }
  return 'attribute_not_exists(version)';
};

const getVersionValues = (item) => {
  if (item.version) {
    return { ':ver': item.version };
  }
  return false;
};

export const BaseModel = class BaseModel {
  constructor({ tableName, keys }) {
    this.tableName = `${process.env.SERVERLESS_STAGE}-${tableName}`;
    this.keys = keys;
  }

  baseAttributes = () => ({
    dateUpdated: Date.now(),
    dateCreated: Date.now()
  });

  getPaginatedResult = (fn, params, cb, maxRequests = 7, request = 1, items = []) => {
    fn.call(dynamodb, params, (err, data) => {
      if (err) {
        cb(err, items);
      } else if (data.LastEvaluatedKey && maxRequests > request) {
        params.ExclusiveStartKey = data.LastEvaluatedKey;
        this.getPaginatedResult(fn, params, cb, maxRequests, request + 1, items.concat(data.Items || []));
      } else {
        if (maxRequests <= request) {
          console.warn(`dynamodb results truncated, maximum number of requests was reached (${maxRequests})`);
        }
        cb(false, items.concat(data.Items || []));
      }
    });
  };

  get = (hashKeyRangePair, consistentRead = true) =>
    new Promise((resolve, reject) =>
      dynamodb.get(
        {
          TableName: this.tableName,
          Key: createKey(this.keys, hashKeyRangePair),
          ConsistentRead: consistentRead
        },
        (error, data) => {
          if (error) reject(error);
          else resolve(data.Item || undefined);
        }
      )
    );

  remove = (hashKeyRangePair) =>
    new Promise((resolve, reject) => {
      dynamodb.delete(
        {
          TableName: this.tableName,
          Key: createKey(this.keys, hashKeyRangePair)
        },
        (error) => {
          if (error) reject(error);
          else resolve();
        }
      );
    });

  removeBatch = (hashKeyRangePairs) =>
    Promise.all(
      chunkArray(hashKeyRangePairs, 25).map((pairs) =>
        dynamodb
          .batchWrite({
            RequestItems: {
              [this.tableName]: pairs.map((k) => {
                return { DeleteRequest: { Key: createKey(this.keys, k) } };
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
    );

  getAllByHash = (hashKeyRangePair, consistentRead = true) => {
    if (!this.keys.rangeKey || (this.keys.rangeKey && hashKeyRangePair.rangeKey)) {
      throw 'getAllByHash only works for Items using both a hash and range key, and querying with only a hash key use get() instead.';
    }
    return this.query(
      `#${this.keys.hashKey} = :${this.keys.hashKey}`,
      { [`#${this.keys.hashKey}`]: this.keys.hashKey },
      { [`:${this.keys.hashKey}`]: hashKeyRangePair.hashKey },
      consistentRead
    );
  };

  getBatch = (hashKeyRangePairs, consistentRead = true) =>
    Promise.all(
      chunkArray(hashKeyRangePairs, 100).map(
        (pairs) =>
          new Promise((resolve, reject) => {
            dynamodb.batchGet(
              {
                RequestItems: {
                  [this.tableName]: {
                    Keys: pairs.map((p) => createKey(this.keys, p)),
                    ConsistentRead: consistentRead
                  }
                }
              },
              (error, data) => {
                if (error) {
                  reject(error);
                } else if (data.Responses && data.Responses[this.tableName]) {
                  resolve(data.Responses[this.tableName]);
                } else {
                  resolve(undefined);
                }
              }
            );
          })
      )
    ).then((results) => {
      return results
        ? results.reduce((acc, res) => {
            if (res) {
              acc = acc.concat(res);
            }
            return acc;
          }, [])
        : results;
    });

  queryIndex = (index, expression, valueMappings, opts = {}) =>
    new Promise((resolve, reject) => {
      this.getPaginatedResult(
        dynamodb.query,
        {
          TableName: this.tableName,
          IndexName: index,
          KeyConditionExpression: expression,
          ExpressionAttributeValues: valueMappings,
          ...opts
        },
        (err, items) => {
          if (err) reject(err);
          else resolve(items);
        }
      );
    });

  // TODO Merge args into a [{ Key, Expression, Value }] structure
  query = (expression, keyMapping, valueMappings, consistentRead = true) =>
    new Promise((resolve, reject) => {
      this.getPaginatedResult(
        dynamodb.query,
        {
          TableName: this.tableName,
          KeyConditionExpression: expression,
          ExpressionAttributeNames: keyMapping,
          ExpressionAttributeValues: valueMappings,
          ConsistentRead: consistentRead
        },
        (err, items) => {
          if (err) reject(err);
          else resolve(items);
        }
      );
    });

  all = () =>
    new Promise((resolve, reject) => {
      this.getPaginatedResult(dynamodb.scan, { TableName: this.tableName }, (err, items) => {
        if (err) reject(err);
        else resolve(items);
      });
    });

  save = (item, userId, conditionExpression) =>
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
        item.dateUpdated = Date.now();
      }
      if (item.updatedBy && userId) {
        item.updatedBy = userId;
      }

      const params = {
        TableName: this.tableName,
        Item: item,
        ReturnValues: 'ALL_OLD',
        ConditionExpression: conditionExpression ? `${conditionExpression} AND ${versionCondition}` : versionCondition
      };
      if (versionValues) {
        params.ExpressionAttributeValues = versionValues;
      }

      dynamodb.put(params, (error, data) => {
        if (error) reject(error);
        else resolve(item);
      });
    });

  saveBatch = (items, userId) =>
    Promise.all(
      chunkArray(items, 25).map((itemBatch) =>
        dynamodb
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
                  item.dateUpdated = Date.now();
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
};

export default { dynamodb, BaseModel };
