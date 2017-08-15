var Importer = require("../index.js");
var program = require("commander");

program
    .usage("<dbPath> <dataPath>")
    .parse(process.argv);

if (program.args.length !== 2) {
    program.help();
}

var dbPath = program.args[0];
var dataPath = program.args[1];

console.log ("dbPath:", dbPath);
console.log ("dataPath:", dataPath);

var i = new Importer();
i.loadData(dataPath, dbPath);