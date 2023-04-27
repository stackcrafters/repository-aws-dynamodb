let jestConfig = require('@stackcrafters/config-babel').jest;
module.exports = {
  ...jestConfig,
  testRegex: '(.*\\.integration\\.test\\.(ts|js))$',
  setupFiles: ['./jestHelpers.ts'],
  globalSetup: './testSetup/integrationSetup.ts',
  // globalTeardown: './testSetup/integrationTeardown.ts'
};
