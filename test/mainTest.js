var Importer = require ("../index.js");
var assert = require ("chai").assert;

describe ("importer", function() {
    it("finds json files", function() {
        var i = new Importer();
        i.findJsonFiles(__dirname + "/data");
        assert.isArray(i.files);
        assert.strictEqual(i.files.length, 10);
    });

    it("connects to neo4j", function() {
        var i = new Importer();
        i.openNeo4j();
        // assert.instanceOf (ret, Promise);
        // return ret;
        assert.isDefined (i.driver);
        i.closeNeo4j();
    });

    it.only("loads data", function() {
        this.timeout(300000);
        var i = new Importer();
        return i.loadData(__dirname + "/data");
    });

    it("loads data from default directory");
});