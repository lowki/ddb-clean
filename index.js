#! /usr/bin/env node
import { Command } from "commander";
import deleteItems from "./delete-items.js";
import scanTable from "./scan-table.js";

const program = new Command();

program
    .version('0.0.1')
    .name('ddb-clean')
    .description('Clean DynamoDB tables');

program.command('scan')
    .description('Scan DynamoDB table and write to files')
    .argument('<tableName>', 'DynamoDB table name')
    .argument('<scanField>', 'Name of the field to filter')
    .argument('<filterMin>', 'Minimum value for the filter')
    .argument('<filterMax>', 'Maximum value for the filter')
    .option('-c, --concurrency <concurrency>', 'Number of concurrent scans (default: 10)')
    .option('-f, --scan-field <scanField>', 'Name of the field to scan (default: timestamp)')
    .option('-o, --output-folder-name <outputFolderName>', 'Name of the output folder (default: batches)')
    .option('-p, --partition-key <partitionKey>', 'Name of the partition key (default: id)')
    .action( async (tableName, scanField, filterMin, filterMax, options) => {
        await scanTable({
            tableName,
            filterMin,
            filterMax,
            scanField,
            concurrency: options.concurrency || 10,
            partitionKey: options.partitionKey || 'id',
            outputFolderName: options.outputFolderName || 'batches'
        });
    });
    
program.command('delete')
    .description('Delete items from DynamoDB table')
    .argument('<tableName>', 'DynamoDB table name')
    .option('-i, --input-folder-name <inputFolder>', 'Name of the input folder (default: batches)')
    .action( async (tableName, options) => {
        await deleteItems({
            tableName,
            inputFolder: options.inputFolder || 'batches'});
    });

program.parse();
