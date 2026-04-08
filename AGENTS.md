# AGENTS.md

## Project Overview

This repository is a small TypeScript library for working with AWS DynamoDB.
Its public surface is exported from `src/index.ts`:

- `BaseModel`: generic DynamoDB repository with CRUD, batch, query, scan, and optimistic-concurrency helpers.
- `CFTableBuilder`: fluent builder for CloudFormation DynamoDB table definitions.
- `dbClient`: shared `DynamoDBDocumentClient`.

The package is published as `@stackcrafters/repository-aws-dynamodb` to GitHub Packages.

## Repo Layout

- `src/`: source of truth. Make code changes here.
- `lib/`: compiled output from Babel/TypeScript. Generated artifact; do not hand-edit.
- `testSetup/`: integration-test seeding/bootstrap helpers.
- `.github/workflows/`: CI for unit tests, integration tests, and package publishing.

## Toolchain

- Node version: `v18` from `.nvmrc`.
- TypeScript declarations emit to `lib/` via `tsc`.
- JavaScript build uses Babel via `@stackcrafters/config-babel`.
- ESLint, Prettier, and Jest config are inherited from `@stackcrafters/config-babel` with local overrides.

## Common Commands

- `npm test`: unit tests only.
- `npm run integ`: integration tests only.
- `npm run type-check`: TypeScript validation without emit.
- `npm run build`: clean, lint, emit declarations, transpile JS.
- `npm run prepublishOnly`: same build used before publish.

## Environment And Runtime Assumptions

- Unit tests set `process.env.SERVERLESS_STAGE = 'stg'` in `jestHelpers.ts`.
- `BaseModel` prefixes table names with `SERVERLESS_STAGE` by default unless `stage: 'postfix'` or `stage: 'none'` is passed.
- Shared DynamoDB client reads `AWS_DYNAMODB_REGION` when present.
- Integration tests assume AWS credentials are available and run against `eu-west-1`.

## Change Guidance

- Prefer editing `src/` and rebuilding instead of touching `lib/` directly.
- Keep the public API backward compatible unless the task clearly calls for a breaking change.
- When changing `BaseModel`, update or add unit tests in `src/BaseModel.test.ts`; many behaviors are already codified there.
- When changing real DynamoDB semantics, also review `src/BaseModel.integration.test.ts` and `testSetup/`.
- Preserve the package's current batching limits unless intentionally changing behavior:
  - `getBatch`: chunks at 100 keys
  - `saveBatch` / `removeBatch`: chunks at 25 requests
  - `query` / `queryIndex` / `all`: pagination capped at 7 requests by default

## Generated And Sensitive Areas

- `lib/` is generated output.
- `.github/workflows/build.yml` is the best reference for CI expectations.
- `.npmrc` points `@stackcrafters` packages at GitHub Packages.
