import fs from 'fs';
import path from 'path';
import ddb from '@aws-sdk/client-dynamodb';
const { DynamoDBClient, BatchWriteItemCommand } = ddb;
import { marshall } from '@aws-sdk/util-dynamodb';
import ora from 'ora';
import chalk from 'chalk';

const dynamodb = new DynamoDBClient();

const chunkArray = (array, size) => {
    const chunkedArray = [];
    for (let i = 0; i < array.length; i += size) {
        chunkedArray.push(array.slice(i, i + size));
    }
    return chunkedArray;
}

/**
 * 
* @param {Object} params
* @param {string} params.tableName DynamoDB table name
* @param {string} params.inputFolder Folder containing the JSON files with the items to delete
 */
export default async ({
    tableName,
    inputFolder,
}) => {
    const files = fs.readdirSync(inputFolder);

    if (files.length === 0) {
        ora().warn(chalk.yellow(`No items found in ${inputFolder}, did you run scan first?`));
        return;
    }

    const spinner = ora(chalk.cyan(`Deleting items from table ${tableName}`)).start();

    for (let file of files) {
        const index = files.indexOf(file);
        spinner.text = chalk.cyan(`Deleting items from table ${tableName} - batch ${index + 1} / ${files.length}`);
        const inputFile = path.join(inputFolder, file);
        const items = JSON.parse(fs.readFileSync(inputFile, 'utf8'));
        const chunks = chunkArray(items, 25);
        await Promise.all(chunks.map(async chunk => {
            /**
             * @type {BatchWriteItemCommandInput}
             */
            const params = {
                RequestItems: {
                    [tableName]: chunk.map(item => ({
                        DeleteRequest: {
                            Key: marshall(item)
                        }
                    }))
                }
            };

            await dynamodb.send(new BatchWriteItemCommand(params));

        }));
        await fs.unlinkSync(inputFile);
    }
    spinner.succeed(chalk.green(`Deleted items from table ${tableName}`));
}
