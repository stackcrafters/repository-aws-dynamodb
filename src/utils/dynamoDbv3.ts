import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { AssumeRoleCommand, STSClient } from '@aws-sdk/client-sts';

const { AWS_DYNAMODB_REGION: region } = process.env;

const client = new DynamoDBClient({ ...(region && { region }) });
export const dbClient = DynamoDBDocumentClient.from(client, {
  marshallOptions: { removeUndefinedValues: true }
});

export type AssumeRoleOpts =
  | {
      roleArn: string;
      roleSessionName: string;
      externalId: string;
      region: string;
    }
  | undefined;

export async function createAssumedDbClient(params: AssumeRoleOpts): Promise<DynamoDBDocumentClient> {
  if (!params) throw new Error('AssumeRole is undefined');
  const sts = new STSClient({ region: params.region });
  const assumeRole = await sts.send(
    new AssumeRoleCommand({
      RoleArn: params.roleArn,
      RoleSessionName: params.roleSessionName || 'dynamodb-per-call',
      ExternalId: params.externalId
    })
  );
  if (!assumeRole.Credentials) throw new Error('AssumeRole returned no credentials');

  const dynamo = new DynamoDBClient({
    region: params.region,
    credentials: {
      accessKeyId: assumeRole.Credentials.AccessKeyId as string,
      secretAccessKey: assumeRole.Credentials.SecretAccessKey as string,
      sessionToken: assumeRole.Credentials.SessionToken as string
    }
  });
  return DynamoDBDocumentClient.from(dynamo, { marshallOptions: { removeUndefinedValues: true } });
}

// Reusable selector for which DocumentClient to use on a call
export const getDbClient = async (params?: AssumeRoleOpts): Promise<DynamoDBDocumentClient> => {
  if (!params || !params?.roleArn) return dbClient;
  return await createAssumedDbClient({
    roleArn: params.roleArn,
    roleSessionName: params.roleSessionName,
    externalId: params.externalId,
    region: params.region
  });
};
