export default {
  ensureExists: {
    'dynamo-integ-test-k1': [
      { id: 'test-get-1', value: 1 },
      { id: 'test-remove-1', value: 1 },
      { id: 'test-batch-remove-1', value: 1 },
      { id: 'test-batch-remove-2', value: 1 },
      { id: 'test-get-batch-1', value: 1 },
      { id: 'test-get-batch-2', value: 1 },
      { id: 'test-put-1', version: 1 },
      { id: 'test-put-3' },
      { id: 'test-put-4', version: 2 },
      { id: 'test-overwrite-1' },
      { id: 'test-overwrite-2' },
      { id: 'test-overwrite-3' },
      { id: 'test-overwrite-4' },
      { id: 'test-update-2', version: 1 },
      { id: 'test-update-3'},
      { id: 'test-update-4', version: 1 },
      { id: 'test-update-5', value: 1 },
      { id: 'test-update-6' },
      { id: 'test-update-7' },
    ],
    'dynamo-integ-test-k2': [
      { id: 'test-get-2', sort: 99, value: 2 },
      { id: 'test-remove-2', sort: 98, value: 2 },
      { id: 'test-batch-remove-3', sort: 97, value: 2 },
      { id: 'test-batch-remove-4', sort: 97, value: 2 },
      { id: 'test-get-batch-2', sort: 96, value: 2 },
      { id: 'test-get-batch-3', sort: 96, value: 2 },
      { id: 'test-put-2', sort: 95, version: 1 },
      { id: 'test-update-1', sort: 93, version: 1, value: 1, createdBy: '100', dateCreated: 1 },
      { id: 'test-query', sort: 1 },
      { id: 'test-query', sort: 2 },
      { id: 'test-query', sort: 3 },
      { id: 'test-query', sort: 4 },
      { id: 'test-queryIndex', sort: 1, idxValue: "abc" },
      { id: 'test-queryIndex', sort: 2, idxValue: "abc" },
      { id: 'test-queryIndex', sort: 3, idxValue: "xyz" },
      { id: 'test-queryIndex', sort: 4, idxValue: "xyz" },
    ]
  },
  ensureAbsent: {
    'dynamo-integ-test-k1': [
      { id: 'non-existent-1' },
      { id: 'non-existent-2' },
      { id: 'test-create-1' },
      { id: 'test-create-3' },
      { id: 'test-create-4' },
      { id: 'test-create-5' },
      { id: 'test-create-batch-1' },
      { id: 'test-create-batch-2' },
      { id: 'test-update-create-1' }
    ],
    'dynamo-integ-test-k2': [
      { id: 'test-create-2', sort: 95 },
      { id: 'test-create-batch-3', sort: 94 },
      { id: 'test-create-batch-4', sort: 94 }
    ]
  }
};
