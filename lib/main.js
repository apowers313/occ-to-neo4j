var dir = require("node-dir");
var stringify = require('csv-stringify');
var fs = require("fs");

class Importer {
    findJsonFiles(path) {
        return this.files = dir.files(path, {
            sync: true
        }).filter((path) => {
            return path.match(/.json$/);
        });
    }

    setDbPath(path) {
        this.dbPath = path;
    }

    loadData(dataPath, dbPath) {
        if (typeof dataPath !== "string") dataPath = process.cwd() + "/data";
        if (typeof dbPath !== "string") dbPath = process.cwd() + "/db";
        this.setDbPath(dbPath);

        Promise.resolve()
            // load BR (bibliographic resource) records
            // XXX NOTE: processing the same file multiple times isn't ideal, but the heap overflows otherwise because streams aren't immediately flushed to disk
            .then(() => {
                this.createCsv("brWriter", "br.csv", ["iri", "label", "type", "title", "subtitle", "year", "number"]);
                this.loadRecords(dataPath + "/br", this.processBr);
                return this.closeCsv("brWriter");
            })
            .then(() => {
                this.createCsv("brPartWriter", "br-part-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrPartOfLinkRecord);
                return this.closeCsv("brPartWriter");
            })
            .then(() => {
                this.createCsv("brIdWriter", "br-id-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrIdLinkRecord);
                return this.closeCsv("brIdWriter");
            })
            .then(() => {
                this.createCsv("brFormatWriter", "br-format-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrFormatLinkRecord);
                return this.closeCsv("brFormatWriter");
            })
            .then(() => {
                this.createCsv("brReferenceWriter", "br-reference-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrReferenceLinkRecord);
                return this.closeCsv("brReferenceWriter");
            })
            .then(() => {
                this.createCsv("brCitationWriter", "br-citation-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrCitationLinkRecord);
                return this.closeCsv("brCitationWriter");
            })
            .then(() => {
                this.createCsv("brContributorWriter", "br-contrib-link.csv", ["src", "dst"]);
                this.loadRecords(dataPath + "/br", this.writeBrContributorLinkRecord);
                return this.closeCsv("brContributorWriter");
            })


            // load identifier records
            .then(() => {
                this.createCsv("idWriter", "id.csv", ["iri", "record_type", "label", "type", "id"]);
                this.loadRecords(dataPath + "/id", this.processId);
                return this.closeCsv("idWriter");
            })
            // load AR (agent roles) records
            .then(() => {
                this.createCsv("arWriter", "ar.csv", ["iri", "record_type", "label", "role_of", "role_type", "next"]);
                this.loadRecords(dataPath + "/ar", this.processAr);
                return this.closeCsv("arWriter");
            })
            // load BE (bibliographic enries) records
            .then(() => {
                this.createCsv("beWriter", "be.csv", ["iri", "record_type", "label", "content", "crossref"]);
                this.loadRecords(dataPath + "/be", this.processBe);
                return this.closeCsv("beWriter");
            })
            // load RE (resource embodiment) records
            .then(() => {
                this.createCsv("reWriter", "re.csv", ["iri", "record_type", "label", "fpage", "lpage"]);
                this.loadRecords(dataPath + "/re", this.processRe);
                return this.closeCsv("reWriter");
            })
            // load RA (responsible agent) records
            .then(() => {
                this.createCsv("raWriter", "ra.csv", ["iri", "record_type", "label", "gname", "fname", "name", "identifier"]);
                this.loadRecords(dataPath + "/ra", this.processRa);
                return this.closeCsv("raWriter");
            });
    }

    loadRecords(jsonPath, writeCsv) {
        console.log("Loading JSON from '" + jsonPath + "'...");

        var files = this.findJsonFiles(jsonPath);
        console.log("Found " + files.length + " files in path:", jsonPath);

        var fileCount = 0,
            recordCount = 0;
        for (let file of files) {
            // console.log("Loading file: '" + file + "'...");
            fileCount++;
            recordCount += this.walkJsonFile(file, writeCsv);
        }
        console.log("Loaded ", recordCount, "records from", fileCount, " files from:", jsonPath);
    }

    createCsv(objName, fileName, headerArr) {
        if (this[objName]) {
            console.log("!!! createCsv:", objName, "already exists!");
            return;
        }
        this[objName] = stringify({
            header: true,
            columns: headerArr
        });

        this[objName].pipe(fs.createWriteStream(this.dbPath + "/import/" + fileName));
        this[objName].on("error", function(err) {
            throw new Error("Writing to CSV stream")
        });
    }

    writeCsv(objName, record) {
        this[objName].write(record);
    }

    closeCsv(objName) {
        return new Promise((resolve) => {
            this[objName].end();
            this[objName].on("finish", function() {
                console.log("closeCsv finished:", objName);
                resolve();
            });
        });
    }

    walkJsonFile(file, cb) {
        if (typeof cb !== "function") {
            throw new TypeError("walkJson: expected callback to be function");
        }

        var json = JSON.parse(fs.readFileSync(file, 'utf8'));
        // var json = require(file);
        // OCC / Blazegraph syntax
        var graph = json["@graph"];
        var i = 0;
        for (let record of graph) {
            cb.call(this, record);
            i++;
        }
        // console.log("Processed " + i + " records.");
        return i;
    }

    processBr(record) {
        var mandatoryList = ["iri", "a"];
        var recognizedList = [
            // "type",
            "iri",
            "label",
            "a",
            "title",
            "subtitle",
            "number",
            "year",
            "part_of",
            "contributor",
            "identifier",
            "format",
            "citation",
            "reference"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeBrRecord(record);
        // this.writeBrPartOfLinkRecord(record);
        // this.writeBrIdLinkRecord(record);
        // this.writeBrFormatLinkRecord(record);
        // this.writeBrContributorLinkRecord(record);
        // this.writeBrReferenceLinkRecord(record);
        // this.writeBrCitationLinkRecord(record);
    }

    validateRecord(record, mandatoryList, recognizedList) {
        for (let i = 0; i < mandatoryList.length; i++) {
            if (record[mandatoryList[i]] === undefined) {
                console.log("!!! mandatory key missing: '" + mandatoryList[i] + "' in record:", record);
            }
        }

        var keys = Object.keys(record);
        for (let i = 0; i < keys.length; i++) {
            if (recognizedList.indexOf(keys[i]) === -1) {
                console.log("!!! unknown key: '" + keys[i] + "' in record:", record);
            }
        }
    }

    writeBrRecord(record) {
        var writeRecord = [
            record.iri,
            record.label,
            getBrType(record),
            record.title,
            record.subtitle,
            record.year,
            record.number,
        ];
        // console.log ("Title:", record.title);

        // console.log ("Writing:", writeRecord);
        this.writeCsv("brWriter", writeRecord);
    }

    writeBrPartOfLinkRecord(record) {
        if (typeof record.part_of !== "string") return;
        var writeRecord = [
            record.iri,
            record.part_of
        ];

        this.writeCsv("brPartWriter", writeRecord);
    }

    writeBrContributorLinkRecord(record) {
        if (typeof record.contributor === "string") record.contributor = [record.contributor];
        if (!Array.isArray(record.contributor)) return;

        for (let i = 0; i < record.contributor.length; i++) {
            let writeRecord = [
                record.iri,
                record.contributor[i]
            ];
            this.writeCsv("brContributorWriter", writeRecord);
        }
    }

    writeBrIdLinkRecord(record) {
        if (typeof record.identifier === "string") record.identifier = [record.identifier];
        if (!Array.isArray(record.identifier)) return;

        for (let i = 0; i < record.identifier.length; i++) {
            let writeRecord = [
                record.iri,
                record.identifier[i]
            ];
            this.writeCsv("brIdWriter", writeRecord);
        }
    }

    writeBrFormatLinkRecord(record) {
        if (Array.isArray(record.format)) {
            console.log("!!! got array format in record:", record);
        }
        if (typeof record.format !== "string") return;
        var writeRecord = [
            record.iri,
            record.format
        ];
        this.writeCsv("brFormatWriter", writeRecord);
    }

    writeBrReferenceLinkRecord(record) {
        if (typeof record.reference === "string") record.reference = [record.reference];
        if (!Array.isArray(record.reference)) return;

        for (let i = 0; i < record.reference.length; i++) {
            let writeRecord = [
                record.iri,
                record.reference[i]
            ];
            this.writeCsv("brReferenceWriter", writeRecord);
        }
    }

    writeBrCitationLinkRecord(record) {
        if (typeof record.citation === "string") record.citation = [record.citation];
        if (!Array.isArray(record.citation)) return;

        for (let i = 0; i < record.citation.length; i++) {
            let writeRecord = [
                record.iri,
                record.citation[i]
            ];
            this.writeCsv("brCitationWriter", writeRecord);
        }
    }

    processId(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "a",
            "label",
            "type",
            "id",
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeIdRecord(record);
    }

    writeIdRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! ID has bad record type:", record);
        }
        var writeRecord = [
            record.iri,
            record.a,
            record.type,
            record.id,
            record.label
        ];
        this.writeCsv("idWriter", writeRecord);
    }

    processAr(record) {
        var mandatoryList = ["iri", "role_of", "role_type"];
        var recognizedList = [
            "iri",
            "a",
            "label",
            "role_of",
            "role_type",
            "next",
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeArRecord(record);

    }

    writeArRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! AR has bad record type:", record);
        }
        var writeRecord = [
            record.iri,
            record.label,
            record.a,
            record.role_of,
            record.role_type,
            record.next,
        ];
        this.writeCsv("arWriter", writeRecord);
    }

    processBe(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "a",
            "label",
            "content",
            "crossref",
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeBeRecord(record);
    }

    writeBeRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! BE has bad record type:", record);
        }
        var writeRecord = [
            record.iri,
            record.label,
            record.a,
            record.content,
            record.crossref
        ];
        this.writeCsv("beWriter", writeRecord);
    }

    processRe(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "a",
            "label",
            "fpage",
            "lpage",
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeReRecord(record);
    }

    writeReRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! RE has bad record type:", record);
        }
        var writeRecord = [
            record.iri,
            record.label,
            record.a,
            record.fpage,
            record.lpage
        ];
        this.writeCsv("reWriter", writeRecord);
    }

    processRa(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "a",
            "label",
            "gname",
            "fname",
            "name",
            "identifier",
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeRaRecord(record);
    }

    writeRaRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! RA has bad record type:", record);
        }
        if (typeof record.identifier !== "string" && record.identifier !== undefined) {
            console.log("!!! RA has bad identifier:", record);
        }
        var writeRecord = [
            record.iri,
            record.label,
            record.a,
            record.gname,
            record.fname,
            record.name,
            record.identifier
        ];
        this.writeCsv("raWriter", writeRecord);
    }
}

function getBrType(record) {
    var list = record.a;

    // there are a few records that are just of type "document"
    if (typeof list === "string") {
        return list;
    }

    for (let i = 0; i < list.length; i++) {
        switch (list[i]) {
            case "document":
                continue;
            case "article":
            case "periodical_issue":
            case "periodical_volume":
            case "periodical_journal":
            case "collection":
            case "reference_entry":
            case "inbook":
            case "book":
            case "inproceedings":
            case "proceedings":
            case "reference_book":
            case "thesis":
            case "standard":
            case "dataset":
            case "techreport":
            case "book_series":
            case "book_part":
            case "series":
                return list[i];
            default:
                console.log("!!! UNKNOWN BR TYPE:", list[i]);
        }
    }
    console.log("!!! COULDN'T FIND BR TYPE IN LIST:", list);
    return "unknown";
}

module.exports = Importer;