#!/usr/bin/env node

const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csv = require('csv-parser');

const casual = require('casual');

function generateRandomData() {
  const generateData = casual.random_element([true, false]);

  if (generateData) {
    return {
      name: casual.full_name,
      email: casual.email,
      address: casual.address,
      phone: casual.phone,
      // Add more fields as needed
    };
  } else {
    return {
      name: '',
      email: '',
      address: '',
      phone: '',
      // Add more fields as needed
    };
  }
}

function generateCsvFile(filename, numRows) {
  const csvWriter = createCsvWriter({
    path: filename,
    header: [
      { id: 'name', title: 'Name' },
      { id: 'email', title: 'Email' },
      { id: 'address', title: 'Address' },
      { id: 'phone', title: 'Phone' },
      // Add more headers as needed
    ],
  });

  const records = [];

  for (let i = 0; i < numRows; i++) {
    records.push(generateRandomData());
  }

  csvWriter.writeRecords(records)
    .then(() => console.log(`CSV file ${filename} written successfully with ${numRows} rows.`))
    .catch((err) => console.error('Error writing CSV file:', err));
}

function processCsvFile(filename) {
  let lineNo = 1;

  fs.createReadStream(filename)
    .pipe(csv())
    .on('data', (row) => {
      // Check if any field in the row is empty
      if (Object.values(row).some(value => value === '')) {
        console.log(`Skipped line ${lineNo}:`, row);
      } else {
        // Process the non-empty row (you can do something with the data here)
        console.log('Processed row:', row);
      }

      lineNo++;
    })
    .on('end', () => {
      console.log(`Finished processing CSV file.`);
    });
}

const args = process.argv.slice(2);

if (args.length !== 2) {
  console.error('Usage: csv-generator <filename> <numRows>');
  process.exit(1);
}

const filename = args[0];
const numRows = parseInt(args[1], 10);

generateCsvFile(filename, numRows);
processCsvFile(filename);
