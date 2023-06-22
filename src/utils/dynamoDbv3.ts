import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

const { AWS_DYNAMODB_REGION: region } = process.env;

const client = new DynamoDBClient({ ...(region && { region }) });
export const dbClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: { removeUndefinedValues: true }
});
