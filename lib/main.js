var dir = require("node-dir");
var neo4j = require("neo4j-driver").v1;

class Importer {
    findJsonFiles(path) {
        return this.files = dir.files(path, {
            sync: true
        }).filter((path) => {
            return path.match(/.json$/);
        });
    }

    openNeo4j() {
        // return new Promise((resolve, reject) => {
        this.driver = neo4j.driver("bolt://localhost", neo4j.auth.basic("neo4j", "passwd"));

        //     driver.onCompleted = function() {
        //         console.log ("Driver connected");
        //         resolve();
        //     };

        //     driver.onError = function(err) {
        //         reject (err);
        //     };
        // });
    }

    closeNeo4j() {
        if (this.driver) {
            this.driver.close();
            this.driver = undefined;
        }
    }

    loadData(path) {
        if (!this.driver) this.openNeo4j();
        if (typeof path !== "string") path = process.cwd() + "/data";
        return Promise.resolve()
            .then(() => {
                return this.loadRecord(path + "/br", createBrQuery);
            })
            // .then(() => {
            //     return this.loadRecord(path + "/id", createIdQuery);
            // });
        // loadAr
        // loadBe
        // loadRe
        // loadRa
    }

    loadRecord(jsonPath, createQuery) {
        console.log("Loading JSON from '" + jsonPath + "'...");

        var files = this.findJsonFiles(jsonPath);
        var session = this.driver.session();

        var p = createIndicies(this.driver);
        var i = 0;
        function addRecord(record) {
            record.name = record.label;
            record.id = createId (record.iri);

            var query = createQuery (record);
            p = p.then(() => {
                if ((++i % 100) === 0) process.stdout.write(".");
                return session.run(query);
            });
        }

        for (let file of files) {
            console.log("Loading file: '" + file + "'...");
            this.walkJsonFile(file, addRecord);
            break;
        }

        p = p.then(() => {
            console.log("\nDone loading '" + jsonPath + "'.");
            session.close();
        });

        return p;
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
            cb(record);
            i++;
        }
        console.log("Processed " + i + " records.");
    }
}

function createIndicies(driver) {
    var session = driver.session();

    return Promise.resolve()
        .then(() => {
            return session.run("CREATE INDEX ON :BibEntry(id)");
        })
        .then(() => {
            return session.run("CREATE INDEX ON :Identifier(id)");
        })
        .then(() => {
            session.close();
        });
}

function createBrQuery(record) {
    // parse out type (article, issue, etc)
    record.type = getBrType(record);

    // add main record
    var query = "";
    query += "MERGE (" + record.id + ":BibEntry {id: \"" + record.id + "\"})";
    query += "SET " + record.id + " = " + createPropString(record) + "\n";

    // add part-of
    if (typeof record.part_of === "string") {
        query += addLink ("PARTOF", record.id, record.part_of, "BibEntry");
    }

    // add resource
    if (typeof record.format === "string") {
        query += addLink ("RESOURCE", record.id, record.format, "Resource");
    }

    // add identifiers
    for (let i = 0; Array.isArray(record.identifier) && i < record.identifier.length; i++) {
        query += addLink ("ID", record.id, record.identifier[i], "Identifier");
    }

    // add citations
    for (let i = 0; Array.isArray(record.citation) && i < record.citation.length; i++) {
        query += addLink ("CITATION", record.id, record.citation[i], "BibEntry");
    }

    // add references
    for (let i = 0; Array.isArray(record.reference) && i < record.reference.length; i++) {
        query += addLink ("REFERENCE", record.id, record.reference[i], "Reference");
    }

    // add contributors
    for (let i = 0; Array.isArray(record.contributor) && i < record.contributor.length; i++) {
        query += addLink ("CONTRIBUTOR", record.id, record.contributor[i], "Contributor");
    }

    console.log("BR QUERY:", query);
    return query;
}

function getBrType(record) {
    var list = record.a;

    // there are a few records that are just of type "document"
    if (typeof list === "string") {
        console.log ("!!! MALFORMED RECORD TYPE:", record);
        return list;
    }

    for (let i = 0; i < list.length; i++) {
        switch(list[i]) {
            case "document":
                continue;
            case "article":
            case "periodical_issue":
            case "periodical_volume":
            case "periodical_journal":
                return list[i];
            default:
                console.log ("!!! UNKNOWN BR TYPE:", list[i]);
        }
    }
    console.log ("!!! COULDN'T FIND BR TYPE IN LIST:", list);
    return "unknown";
}

function createIdQuery(record) {
    var query = "MERGE (id:Identifier {id: \"" + record.id + "\"})\n";
    query += "SET id += " + createPropString(record);
    console.log("ID QUERY:", query);
    return query;
}

function createId(id) {
    return id.replace(":", "");
}

function addLink(type, srcId, newId, dstType) {
        var dstId = createId(newId);
        var query = "";
        query += "MERGE (" + dstId + ":" + dstType + " {id: \"" + dstId + "\"})\n";
        query += "MERGE (" + srcId + ")-[:" + type + "]->(" + dstId + ")\n";
        return query;
}

function createPropString(obj) {
    var ret = "";
    var keys = Object.keys(obj);
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        let comma = (i !== (keys.length - 1)) ? ", " : "";
        // console.log("key", key);
        ret += key + ": ";
        let data = obj[key];

        // escape quotes
        if (typeof data === "string" && data.search(/"/) !== -1) {
            data = data.replace(/"/g, '\\"');
        }
        ret += '"' + data + '"' + comma;
    }

    // console.log("ret", ret);

    return "{ " + ret + " }";
}

module.exports = Importer;