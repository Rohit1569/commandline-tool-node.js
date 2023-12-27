#!/usr/bin/env node






const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const casual = require('casual');
const yargs = require('yargs');
const { Readable } = require('stream');

function generateRandomData(columns, additionalColumns) {
  const rowData = {};

  columns.forEach(column => {
    if (!casual[column]) {
      casual.define(column, () => casual.word);
    }

    rowData[column] = casual[column];
  });

  additionalColumns.forEach(column => {
    if (!casual[column]) {
      casual.define(column, () => casual.word);
    }

    rowData[column] = casual[column];
  });

  return rowData;
}

async function generateAndWriteBatch(csvWriter, records) {
  await csvWriter.writeRecords(records);
  console.log(`Batch generated with ${records.length} records`);
}

async function generateCsvFile(filename, numRows, columns, additionalColumns, outputFile) {
  const allColumns = [...columns, ...additionalColumns];
  const outputPath = filename;

  const batchSize = 10000;
  const csvWriter = createCsvWriter({
    path: outputPath,
    header: allColumns.map(column => ({ id: column, title: column })),
  });

  for (let i = 0; i < numRows; i += batchSize) {
    const endIndex = Math.min(i + batchSize, numRows);
    const records = [];
    for (let j = i; j < endIndex; j++) {
      records.push(generateRandomData(columns, additionalColumns));
    }
    await generateAndWriteBatch(csvWriter, records);
  }

  console.log(`CSV file ${outputPath} written successfully with ${numRows} rows.`);
}

async function processCsvFile(filename, columns) {
  let lineNo = 1;
  let skippedLines = 0;

  fs.createReadStream(filename)
    .pipe(csv())
    .on('data', (row) => {
      if (columns.some(column => row[column] === '')) {
        console.log(`Skipped line ${lineNo}:`, row);
        skippedLines++;
      }

      lineNo++;
    })
    .on('end', () => {
      console.log(`Finished processing CSV file. Skipped ${skippedLines} lines with empty data in columns '${columns.join(', ')}'.`);
    });
}

async function generateFromExistingCsv(inputFile, outputFile = 'output.csv', columns) {
  const csvWriter = createCsvWriter({
    path: outputFile,
    header: columns.map(column => ({ id: column, title: column })),
  });

  const batchSize = 10000;
  let records = [];

  const processBatch = async () => {
    if (records.length > 0) {
      await generateAndWriteBatch(csvWriter, records);
      records = [];
    }
  };

  return new Promise((resolve, reject) => {
    fs.createReadStream(inputFile)
      .pipe(csv())
      .on('data', (row) => {
        if (!columns.some(column => row[column] === '')) {
          records.push(row);
        }

        if (records.length === batchSize) {
          processBatch();
        }
      })
      .on('end', async () => {
        await processBatch();

        console.log(`New CSV file ${outputFile} generated successfully.`);
        resolve();
      })
      .on('error', (error) => {
        console.error('Error reading CSV file:', error);
        reject(error);
      });
  });
}

const argv = yargs
  .command('generate', 'Generate a new CSV file', (yargs) => {
    yargs.options({
      'c': { alias: 'columns', describe: 'Columns to generate (comma-separated)', demandOption: true, type: 'string' },
      'add-columns': { describe: 'Additional columns to generate (comma-separated)', type: 'string' },
      'n': { alias: 'num-rows', describe: 'Number of rows to generate', demandOption: true, type: 'number' },
      'o': { alias: 'output', describe: 'Output file name', type: 'string', default: 'output.csv' },
      'f': { alias: 'filename', describe: 'Input CSV file', type: 'string' },
    });
  })
  .command('process', 'Process an existing CSV file', (yargs) => {
    yargs.options({
      'f': { alias: 'filename', describe: 'CSV file to process', demandOption: true, type: 'string' },
      'c': { alias: 'columns', describe: 'Columns to check for empty data (comma-separated)', demandOption: true, type: 'string' },    
    });
  })
  .command('add-columns', 'Add additional columns to an existing CSV file', (yargs) => {
    yargs.options({
      'f': { alias: 'filename', describe: 'CSV file to update', demandOption: true, type: 'string' },
      'add-columns': { describe: 'Additional columns to add (comma-separated)', demandOption: true, type: 'string' },
    });
  })
  .command('generate-from-existing', 'Generate a new CSV file from an existing CSV file excluding rows with empty fields', (yargs) => {
    yargs.options({
      'i': { alias: 'input', describe: 'Input CSV file', demandOption: true, type: 'string' },
      'o': { alias: 'output', describe: 'Output file name', type: 'string', default: 'output.csv' },
      'c': { alias: 'columns', describe: 'Columns to check for empty data (comma-separated)', demandOption: true, type: 'string' },
    });
  })
  .demandCommand(1, 'Please specify a command')
  .help()
  .argv;

const command = argv._[0];

if (argv.columns) {
  argv.columns.split(',').forEach(column => {
    if (!casual[column]) {
      casual.define(column, () => casual.word);
    }
  });
}

switch (command) {
  case 'generate':
    const generateColumns = argv.columns.split(',');
    const generateAdditionalColumns = argv['add-columns'] ? argv['add-columns'].split(',') : [];
    generateCsvFile(argv.filename, argv['num-rows'], generateColumns, generateAdditionalColumns, argv.output);
    break;

  case 'process':
    const processColumns = argv.columns.split(',');
    processCsvFile(argv.filename, processColumns);
    break;

  case 'add-columns':
    const addColumns = argv['add-columns'].split(',');
    console.log(`Additional columns to add: ${addColumns.join(', ')}`);
    // Add logic to update the existing CSV file with additional columns
    break;

  case 'generate-from-existing':
    generateFromExistingCsv(argv.input, argv.output, argv.columns.split(','));
    break;

  default:
    console.error('Invalid command. Please use "generate", "process", "add-columns", or "generate-from-existing".');
}
