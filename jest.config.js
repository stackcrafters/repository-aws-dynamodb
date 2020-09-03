module.exports = {
  roots: ['<rootDir>/src'],
  testRegex: '(.*\\.test\\.(js?))$',
  transform: {
    '^.+\\.js?$': 'babel-jest'
  },
  moduleFileExtensions: ['js', 'jsx', 'json', 'node']
};
