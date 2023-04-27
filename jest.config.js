let jestConfig = require('@stackcrafters/config-babel').jest;
module.exports = {
  ...jestConfig,
  testRegex: '(.*\\.test\\.(ts|js))$',
  testPathIgnorePatterns: ['.*\\.integration\\.test\\.(js|ts)'],
  setupFiles: ['./jestHelpers.ts']
};
