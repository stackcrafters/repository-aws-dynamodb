let jestConfig = require('@stackcrafters/config-babel').jest;
module.exports = {
    ...jestConfig,
    setupFiles: ['./jestHelpers.ts']
};
