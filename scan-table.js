import ddb from '@aws-sdk/client-dynamodb';
const { ScanCommandInput } = ddb;
import {parallelScanAsStream} from '@shelf/dynamodb-parallel-scan';
import fs from 'fs';
import ora from 'ora';
import chalk from 'chalk';

/**
 * 
* @param {Object} params
* @param {string} params.tableName DynamoDB table name
* @param {number} params.filterMin Minimum value for the filter
* @param {number} params.filterMax Maximum value for the filter
* @param {number} params.segment Current segment value
* @param {number} params.totalSegments Total segments value
* @param {string} params.scanField Name of the field to scan (default: 'timestamp')
* @param {string} params.partitionKey Name of the partition key (default: 'id')
* @param {number} params.concurrency Number of concurrent scans (default: 10)
* @returns {Promise<Object>} 
 */
export default async ({
    tableName,
    filterMin,
    filterMax, 
    concurrency = 10,
    scanField = 'timestamp', 
    partitionKey = 'id',
    outputFolderName = 'outputs'
}) => {

    if (!fs.existsSync(outputFolderName)){
        fs.mkdirSync(outputFolderName);
    }
    const spinner = ora(chalk.cyan(`Scanning table ${tableName}`)).start();

    /**
     * @type {ScanCommandInput}
     */
    const scanParams = {
        TableName: tableName,
        ProjectionExpression: `#partitionKey, #scanField`,
          FilterExpression: '#scanField BETWEEN :filterMin AND :filterMax',
          ExpressionAttributeNames: {
              '#scanField': scanField,
              '#partitionKey': partitionKey
          },
          ExpressionAttributeValues: {
              ':filterMin': parseInt(filterMin),
              ':filterMax': parseInt(filterMax)
          }
    };
    const stream = await parallelScanAsStream(scanParams, { concurrency, chunkSize: 1000, highWaterMark: 1000 });
    let count = 0;
    for await (const item of stream) {
        // Write to file
        count++;
        await fs.writeFileSync(`${outputFolderName}/items-batch-${count}.json`, JSON.stringify(item));
    }
    if (count === 0) {
        spinner.warn(chalk.yellow(`No items found for ${tableName}`));
        return;
    }
    spinner.succeed(chalk.green(`Scanned finished, saved ${count} files in ${outputFolderName} folder`));
    

};