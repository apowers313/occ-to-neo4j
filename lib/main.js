var dir = require("node-dir");
var csvWriter = require("csv-write-stream");
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

        // load BR (bibliographic resource) records
        this.createCsv("brWriter", "br.csv", ["iri", "type", "title", "subtitle", "year", "number", "label"]);
        this.createCsv("brContributorWriter", "br-contrib-link.csv", ["src", "dst"]);
        this.createCsv("brPartWriter", "br-part-link.csv", ["src", "dst"]);
        this.createCsv("brIdWriter", "br-id-link.csv", ["src", "dst"]);
        this.createCsv("brFormatWriter", "br-format-link.csv", ["src", "dst"]);
        this.createCsv("brReferenceWriter", "br-reference-link.csv", ["src", "dst"]);
        this.createCsv("brCitationWriter", "br-citation-link.csv", ["src", "dst"]);
        this.loadRecords(dataPath + "/br", this.processBr);
        this.closeCsv("brWriter");
        this.closeCsv("brContributorWriter");
        this.closeCsv("brPartWriter");
        this.closeCsv("brIdWriter");
        this.closeCsv("brFormatWriter");
        this.closeCsv("brReferenceWriter");
        this.closeCsv("brCitationWriter");

        // load identifier records
        this.createCsv("idWriter", "id.csv", ["iri", "record_type", "type", "id", "label"]);
        this.loadRecords(dataPath + "/id", this.processId);
        this.closeCsv("idWriter");

        // load AR (agent roles) records
        this.createCsv("arWriter", "ar.csv", ["iri", "record_type", "label", "role_of", "role_type", "next"]);
        this.loadRecords(dataPath + "/ar", this.processAr);
        this.closeCsv("arWriter");

        // load BE (bibliographic enries) records
        this.createCsv("beWriter", "be.csv", ["iri", "record_type", "label", "content", "crossref"]);
        this.loadRecords(dataPath + "/be", this.processBe);
        this.closeCsv("beWriter");

        // load RE (resource embodiment) records
        this.createCsv("reWriter", "re.csv", ["iri", "record_type", "label", "fpage", "lpage"]);
        this.loadRecords(dataPath + "/re", this.processRe);
        this.closeCsv("reWriter");

        // load RA (responsible agent) records
        this.createCsv("raWriter", "ra.csv", ["iri", "record_type", "label", "given_name", "family_name", "name", "identifier"]);
        this.loadRecords(dataPath + "/ra", this.processRa);
        this.closeCsv("raWriter");
    }

    loadRecords(jsonPath, writeCsv) {
        console.log("Loading JSON from '" + jsonPath + "'...");

        var files = this.findJsonFiles(jsonPath);

        for (let file of files) {
            console.log("Loading file: '" + file + "'...");
            this.walkJsonFile(file, writeCsv);
        }
    }

    createCsv(objName, fileName, headerArr) {
        if (!this[objName]) {
            this[objName] = csvWriter({
                headers: headerArr
            });
            this[objName].pipe(fs.createWriteStream(this.dbPath + "/import/" + fileName));
        }
    }

    closeCsv(objName) {
        this[objName].end();
    }

    walkJsonFile(file, cb) {
        if (typeof cb !== "function") {
            throw new TypeError("walkJson: expected callback to be function");
        }

        var json = require(file);
        // OCC / Blazegraph syntax
        var graph = json["@graph"];
        var i = 0;
        for (let record of graph) {
            cb.call(this, record);
            i++;
        }
        console.log("Processed " + i + " records.");
    }

    processBr(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "type", "title", "subtitle", "number", "year", "label", "iri", "a", "part_of", "contributor", "identifier", "format", "citation", "reference"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeBrRecord(record);
        this.writeBrContributorLinkRecord(record);
        this.writeBrPartOfLinkRecord(record);
        this.writeBrIdLinkRecord(record);
        this.writeBrFormatLinkRecord(record);
        this.writeBrReferenceLinkRecord(record);
        this.writeBrCitationLinkRecord(record);
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
        var writeRecord = {
            record_type: getBrType(record),
            title: record.title,
            subtitle: record.subtitle,
            year: record.year,
            number: record.number,
            label: record.label,
            iri: record.iri
        };

        // console.log ("Writing:", writeRecord);
        this.brWriter.write(writeRecord);
    }

    writeBrPartOfLinkRecord(record) {
        if (typeof record.part_of !== "string") return;
        var writeRecord = {
            src: record.iri,
            dst: record.part_of
        };

        this.brPartWriter.write(writeRecord);
    }

    writeBrContributorLinkRecord(record) {
        if (typeof record.contributor === "string") record.contributor = [record.contributor];
        if (!Array.isArray(record.contributor)) return;

        for (let i = 0; i < record.contributor.length; i++) {
            let writeRecord = {
                src: record.iri,
                dst: record.contributor[i]
            };
            this.brContributorWriter.write(writeRecord);
        }
    }

    writeBrIdLinkRecord(record) {
        if (typeof record.identifier === "string") record.identifier = [record.identifier];
        if (!Array.isArray(record.identifier)) return;

        for (let i = 0; i < record.identifier.length; i++) {
            let writeRecord = {
                src: record.iri,
                dst: record.identifier[i]
            };
            this.brIdWriter.write(writeRecord);
        }
    }

    writeBrFormatLinkRecord(record) {
        if (Array.isArray(record.format)) {
            console.log("!!! got array format in record:", record);
        }
        if (typeof record.format !== "string") return;
        var writeRecord = {
            src: record.iri,
            dst: record.format
        };
        this.brFormatWriter.write(writeRecord);
    }

    writeBrReferenceLinkRecord(record) {
        if (typeof record.reference === "string") record.reference = [record.reference];
        if (!Array.isArray(record.reference)) return;

        for (let i = 0; i < record.reference.length; i++) {
            let writeRecord = {
                src: record.iri,
                dst: record.reference[i]
            };
            this.brReferenceWriter.write(writeRecord);
        }
    }

    writeBrCitationLinkRecord(record) {
        if (typeof record.citation === "string") record.citation = [record.citation];
        if (!Array.isArray(record.citation)) return;

        for (let i = 0; i < record.citation.length; i++) {
            let writeRecord = {
                src: record.iri,
                dst: record.citation[i]
            };
            this.brCitationWriter.write(writeRecord);
        }
    }

    processId(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "label",
            "a",
            "type",
            "id"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeIdRecord(record);
    }

    writeIdRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! ID has bad record type:", record);
        }
        var writeRecord = {
            iri: record.iri,
            record_type: record.a,
            type: record.type,
            id: record.id,
            label: record.label
        };
        this.idWriter.write(writeRecord);
    }

    processAr(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "label",
            "a",
            "role_of",
            "next",
            "role_type"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeArRecord(record);

    }

    writeArRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! AR has bad record type:", record);
        }
        var writeRecord = {
            iri: record.iri,
            label: record.label,
            record_type: record.a,
            role_of: record.role_of,
            role_type: record.role_type,
            next: record.next,
        };
        this.arWriter.write(writeRecord);
    }

    processBe(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "label",
            "a",
            "content",
            "crossref"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeBeRecord(record);
    }

    writeBeRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! BE has bad record type:", record);
        }
        var writeRecord = {
            iri: record.iri,
            label: record.label,
            record_type: record.a,
            content: record.content,
            crossref: record.crossref
        };
        this.beWriter.write(writeRecord);
    }

    processRe(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "label",
            "a",
            "fpage",
            "lpage"
        ];
        this.validateRecord(record, mandatoryList, recognizedList);
        this.writeReRecord(record);
    }

    writeReRecord(record) {
        if (typeof record.a !== "string") {
            console.log("!!! RE has bad record type:", record);
        }
        var writeRecord = {
            iri: record.iri,
            label: record.label,
            record_type: record.a,
            fpage: record.fpage,
            lpage: record.lpage
        };
        this.reWriter.write(writeRecord);
    }

    processRa(record) {
        var mandatoryList = ["iri"];
        var recognizedList = [
            "iri",
            "label",
            "a",
            "gname",
            "fname",
            "identifier",
            "name"
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
        var writeRecord = {
            iri: record.iri,
            label: record.label,
            record_type: record.a,
            given_name: record.gname,
            family_name: record.fname,
            name: record.name,
            identifier: record.identifier
        };
        this.raWriter.write(writeRecord);
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
                return list[i];
            default:
                console.log("!!! UNKNOWN BR TYPE:", list[i]);
        }
    }
    console.log("!!! COULDN'T FIND BR TYPE IN LIST:", list);
    return "unknown";
}

module.exports = Importer;