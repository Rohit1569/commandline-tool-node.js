#!/usr/bin/env node
// process-csv.js

const fs = require('fs');
const csv = require('csv-parser');

// function processCsvFile(filename) {
//   let lineNo = 1;
//   let skippedLines = 0;

//   fs.createReadStream(filename)
//     .pipe(csv())
//     .on('data', (row) => {
//       // Check if any field in the row is empty
//       if (Object.values(row).some(value => value === '')) {
//         console.log(`Skipped line ${lineNo}:`, row);
//         skippedLines++;
//       }

//       lineNo++;
//     })
//     .on('end', () => {
//       console.log(`Finished processing CSV file. Skipped ${skippedLines} lines with empty data.`);
//     });
// }

function processCsvFile(filename, columns, fieldToCheck) {
    let lineNo = 1;
    let skippedLines = 0;
  
    fs.createReadStream(filename)
      .pipe(csv())
      .on('data', (row) => {
        // Check if the specified field is empty
        if (row[fieldToCheck] === '') {
          console.log(`Skipped line ${lineNo}: ${fieldToCheck} is empty`);
          skippedLines++;
          return; // Skip the entire line
        }
  
        // Check if any specified column (other than the specified field) in the row is empty
        if (columns.some(column => column !== fieldToCheck && row[column] === '')) {
          console.log(`Skipped line ${lineNo}:`, row);
          skippedLines++;
        }
  
        lineNo++;
      })
      .on('end', () => {
        console.log(`Finished processing CSV file. Skipped ${skippedLines} lines with empty data in columns '${columns.join(', ')}' and field '${fieldToCheck}'.`);
      });
  }
  


const args = process.argv.slice(2);

if (args.length !== 1) {
  console.error('Usage: process-csv <filename>');
  process.exit(1);
}

const filename = args[0];

processCsvFile(filename);
