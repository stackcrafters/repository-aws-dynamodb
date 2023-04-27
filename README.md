# repository-aws-dynamodb

## BaseModel

This library provides a `BaseModel` class that can be extended in order to model, and then easily query for and modify
data in your AWS DynamoDB tables.

You'll need to define each of your table models like so:

```ts
export interface Invitation extends BaseObject {
  id: string;
  email: string;
  code: string;
  expires?: number;
  createdBy?: string;
  updatedBy?: string;
}

class InvitationModel extends BaseModel<Invitation> {
  constructor() {
    super({
      tableName: 'invitation',
      keys: {
        hashKey: 'id',
        rangeKey: 'dateCreated',
        globalIndexes: {
          'code-index': {
            hashKey: 'code'
          }
        }
      }
    });
  }
}
```

Subsequently, you can create a singleton instance of your class, in order to call one of the several class functions available.

When calling `super` in your class constructor, you can also override some default options:

- `config.dateUnits`: `ms` for millisecond timestamps, `s` for second timestamps (default `s`).
- `stage`: ('postfix' | 'prefix' | 'none'), whether the current `env.SERVERLESS_STAGE` should be prepended or appended to the tableName.

It's recommended to also create a function on your class that would be responsible for creating a new instance of an object
to be persisted, in order to ensure default field values are set consistently.

Getter and query functions expect an object containing the `hashKey` (and `rangeKey` if applicable) defined in the model.

the `queryIndex` function expects an object containing the keys defined in the relevant `globalIndex`.

By default, query functions will paginate up to 7 requests to DynamoDB before logging a warning and returning a truncated result set.

Write operations will automatically set/update `createdBy`, `updatedBy`, `dateUpdated` and `dateCreated` properties on the saved
object where possible/appropriate.

## CFTableBuilder

The CFTableBuilder provides a builder class to allow easy creation of DynamoDB table definitions for CloudFormation templates.

e.g.

```js
{
  TableInvitation: new CFTableBuilder('${self:provider.stage}-invitation')
    .attribute({ name: 'id', type: 'S', key: 'HASH' })
    .attribute({ name: 'dateCreated', type: 'N', key: 'RANGE' })
    .attribute({ name: 'code', type: 'S' })
    .ttl('expires')
    .globalSecondaryIndex('code-index')
    .key({ name: 'code', key: 'HASH' })
    .build();
}
```
