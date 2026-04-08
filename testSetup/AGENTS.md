# AGENTS.md

## Purpose

This directory supports the integration test suite in `src/BaseModel.integration.test.ts`.
It is designed to make the test tables converge to a known state before the suite runs.

## Files

- `integrationSetup.ts`: Jest global setup that seeds and deletes data before integration tests.
- `data.ts`: fixture source of truth.
  - `ensureExists`: records that must be present before tests run.
  - `ensureAbsent`: records that must be removed before tests run.
- `util.ts`: builds DynamoDB `BatchWriteCommand` request payloads in chunks of 25.
- `create-tables.sh`: manual helper to create the two expected integration tables.

## Environment Assumptions

- Setup targets AWS region `eu-west-1`.
- Integration tests expect real AWS credentials with access to DynamoDB and the test tables.
- Table names are hard-coded and intentionally use `stage: 'none'` in the integration test models:
  - `dynamo-integ-test-k1`
  - `dynamo-integ-test-k2`

## Extending Integration Tests

- Add or adjust fixture rows in `data.ts` when a new test depends on pre-existing or explicitly absent data.
- Keep fixtures idempotent so repeated runs converge on the same state.
- If a new table or index is introduced for integration coverage, update both:
  - `create-tables.sh`
  - `data.ts` / `integrationSetup.ts` inputs as needed
- Respect DynamoDB batch-write limits when changing setup code; the current helper chunks requests at 25.

## Practical Notes

- `integrationSetup.ts` seeds existing rows and deletes absent rows in parallel with `BatchWriteCommand`.
- The setup is intentionally lightweight; there is no teardown file enabled right now.
- CI runs integration tests after configuring AWS credentials in `.github/workflows/build.yml`.
