type KeyType = 'HASH' | 'RANGE';
type Attribute = { name: string; type: 'S' | 'N' | 'B'; key?: KeyType; ttl?: boolean };
type IndexKey = { name: string; key: KeyType };

type ProjectionType = 'KEYS_ONLY' | 'INCLUDE' | 'ALL';
type StreamViewType = 'KEYS_ONLY' | 'NEW_IMAGE' | 'OLD_IMAGE' | 'NEW_AND_OLD_IMAGES';

type IndexEntry = { AttributeName: string; KeyType: KeyType };

class IndexBuilder {
  IndexName: string;
  KeySchema: IndexEntry[];
  Projection: { ProjectionType: ProjectionType; NonKeyAttributes?: string[] };
  parent: CFTableBuilder;

  constructor(name: string, parent: CFTableBuilder) {
    this.IndexName = name;
    this.KeySchema = [];
    this.Projection = { ProjectionType: 'KEYS_ONLY' };
    this.parent = parent;
  }

  key(attr: IndexKey) {
    this.KeySchema.push({
      AttributeName: attr.name,
      KeyType: attr.key
    });
    return this;
  }

  projection(type: ProjectionType, attributes?: string[]) {
    this.Projection = { ProjectionType: type };
    if (attributes) {
      this.Projection.NonKeyAttributes = attributes;
    }
    return this;
  }

  and() {
    return this.parent;
  }

  build() {
    return this.parent.build();
  }
}

export default class CFTableBuilder {
  Type: string;
  DeletionPolicy: 'Retain' | 'Delete' | 'Snapshot';
  Properties: any;

  constructor(tableName: string) {
    this.Type = 'AWS::DynamoDB::Table';
    this.DeletionPolicy = 'Retain';
    this.Properties = {
      TableName: tableName,
      BillingMode: 'PAY_PER_REQUEST',
      SSESpecification: { SSEEnabled: true },
      PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
      AttributeDefinitions: [],
      KeySchema: []
    };
  }

  attribute(attr: Attribute) {
    this.Properties.AttributeDefinitions.push({
      AttributeName: attr.name,
      AttributeType: attr.type
    });
    if (attr.key !== undefined) {
      this.Properties.KeySchema.push({
        AttributeName: attr.name,
        KeyType: attr.key
      });
    }
    return this;
  }

  ttl(attributeName: string, enabled = true) {
    this.Properties.TimeToLiveSpecification = {
      AttributeName: attributeName,
      Enabled: enabled
    };
    return this;
  }

  stream(streamViewType: StreamViewType) {
    this.Properties.StreamSpecification = {
      StreamViewType: streamViewType
    };
    return this;
  }

  globalSecondaryIndex(name: string) {
    const indexes = this.Properties.GlobalSecondaryIndexes || [];
    this.Properties.GlobalSecondaryIndexes = indexes;
    const indexBuilder = new IndexBuilder(name, this);
    indexes.push(indexBuilder);
    return indexBuilder;
  }

  build() {
    const gsiEntries = this.Properties.GlobalSecondaryIndexes?.map((i) => {
      const { parent, ...entry } = i;
      return entry;
    });
    return { ...this, Properties: { ...this.Properties, GlobalSecondaryIndexes: gsiEntries } };
  }
}
