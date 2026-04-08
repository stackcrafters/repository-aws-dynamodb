# AGENTS.md

## Source Map

- `BaseModel.ts`: central repository abstraction. Most behavioral work lands here.
- `cloudformation/CFTableBuilder.ts`: fluent CloudFormation table-definition builder.
- `utils/dynamoDbv3.ts`: shared `DynamoDBDocumentClient` plus optional STS assume-role client creation.
- `utils/array.ts`: generic chunking helper used for DynamoDB batch limits.
- `utils/date.ts`: second-based timestamp helper used by default timestamp logic.
- `index.ts`: package export barrel.

## BaseModel Notes

`BaseModel<T extends BaseObject>` is the core of the package.
Key design points:

- Table naming is resolved in the constructor from `tableName` plus `stage`.
- Keys are declared up front with `hashKey`, optional `rangeKey`, and optional named `globalIndexes`.
- `createKey()` extracts only the configured key fields from a passed object.
- `prepareSave()` and `prepareUpdateV2()` implement optimistic concurrency using `version`.
- Timestamp/user metadata is only touched when the relevant properties already exist on the item:
  - `dateUpdated`
  - `updatedBy`
  - `dateCreated` / `createdBy` for `updateV2`

## Important Behavioral Details

- `save()` increments `item.version` in-place before issuing `PutCommand`.
- `saveBatch()` does not apply optimistic concurrency checks; it writes raw `PutRequest` batches.
- `get()` swallows read errors and returns `undefined`, logging a warning.
- `query()` and `queryIndex()` use paginator helpers and return `{ items, lastEvaluatedKey }`.
- `all()` scans with the same paginator helper and returns the collected items.
- `update()` is a low-level wrapper around `UpdateCommand`.
- `updateV2()` is the higher-level update path that composes:
  - version checks
  - `dateCreated` / `dateUpdated`
  - optional `createdBy` / `updatedBy`
  - caller-provided expressions merged with generated expressions

## When Editing BaseModel

- Keep the unit tests in `BaseModel.test.ts` aligned with any change to expressions, batching, or table-name behavior.
- Keep the integration tests in `BaseModel.integration.test.ts` aligned with any change to live DynamoDB behavior.
- Be careful with mutations: several helpers intentionally mutate the passed item object.
- Maintain compatibility with AWS SDK v3 command input shapes already used across the tests.

## CloudFormation Builder Notes

`CFTableBuilder` is a chainable builder that emits CloudFormation-style objects, not AWS SDK requests.

- Default table settings include:
  - `BillingMode: PAY_PER_REQUEST`
  - point-in-time recovery enabled
  - SSE disabled
  - `DeletionPolicy: Retain`
- `globalSecondaryIndex()` returns an `IndexBuilder`; callers return to the parent with `.and()` or `.build()`.
- `build()` strips the internal `parent` reference from GSI entries before returning the final object.

## Client Utility Notes

- `dbClient` is shared and configured with `marshallOptions.removeUndefinedValues = true`.
- `getDbClient()` returns the shared client unless a role ARN is provided.
- `createAssumedDbClient()` uses STS `AssumeRole` and creates a per-call DynamoDB client with temporary credentials.
