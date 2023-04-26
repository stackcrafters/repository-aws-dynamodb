import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchWriteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import data from './data';
import { buildBatchRequests } from './util';

const client = new DynamoDBClient({ region: 'eu-west-1' });
const docClient = DynamoDBDocumentClient.from(client);

export default async (globalConfig, projectConfig) => {
  const createItems = buildBatchRequests(data.ensureExists, 'PutRequest');
  const deleteItems = buildBatchRequests(data.ensureAbsent, 'DeleteRequest', 'Key');

  await Promise.all(
    createItems.concat(deleteItems).map(
      async (i) =>
        await docClient.send(
          new BatchWriteCommand({
            RequestItems: i
          })
        )
    )
  );
};
