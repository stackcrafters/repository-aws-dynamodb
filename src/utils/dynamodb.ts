import AWS from 'aws-sdk';
const dynamodb = new AWS.DynamoDB.DocumentClient();
export default dynamodb;
