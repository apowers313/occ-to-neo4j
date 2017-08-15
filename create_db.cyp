// *************** create nodes ***************

// load BR (bibliographic resource) records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br.csv" AS row
CREATE (:BibliographicResource {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    title: row.title,
    subtitle: row.subtitle,
    number: row.number,
    year: row.year
    })
RETURN "Created Bibliographic Resource (BR) records:" AS Result, count(row) AS ` `;

// load ID records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///id.csv" AS row
CREATE (:Identifier {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    type: row.type,
    id: row.id
    })
RETURN "Created ID records:" AS Result, count(row) AS ` `;

// load RA (responsible agent) records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///ra.csv" AS row
CREATE (:Agent {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    given_name: row.given_name,
    family_name: row.family_name,
    name: row.name,
    identifier: row.identifier
    })
RETURN "Created Responsible Agent (RA) records:" AS Result, count(row) AS ` `;

// load AR (agent role) records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///ar.csv" AS row
CREATE (:Role {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    next: row.next,
    role_type: row.role_type,
    role_of: row.role_of
    })
RETURN "Created Agent Role (AR) records:" AS Result, count(row) AS ` `;

// load RE (resource embodiment) records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///re.csv" AS row
CREATE (:ResourceEmbodiment {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    fpage: row.fpage,
    lpage: row.lpage
    })
RETURN "Created Resource Embodiment (RE) records:" AS Result, count(row) AS ` `;

// load BE (bibliographic entry) records
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///be.csv" AS row
CREATE (:BibliographicEntry {
    iri: row.iri,
    label: row.label,
    record_type: row.record_type,
    content: row.content,
    crossref: row.crossref
    })
RETURN "Created Bibliographic Entry (BE) records:" AS Result, count(row) AS ` `;

// *************** create indicies ***************

CREATE INDEX ON :BibliographicResource(iri);
CREATE INDEX ON :Identifier(iri);
CREATE INDEX ON :Agent(iri);
CREATE INDEX ON :Role(iri);
CREATE INDEX ON :ResourceEmbodiment(iri);
CREATE INDEX ON :BibliographicEntry(iri);
RETURN "Created iri indicies" AS ` `;

// *************** create links ***************

// add part-of links
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-part-link.csv" AS row
MATCH (src:BibliographicResource {iri: row.src})
MATCH (dst:BibliographicResource {iri: row.dst})
MERGE (src)-[:PARTOF]->(dst)
RETURN "Created BR part-of links:" AS Result, count(row) AS ` `;

// add ID links
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-id-link.csv" AS row
MATCH (src:BibliographicResource {iri: row.src})
MATCH (dst:Identifier {iri: row.dst})
MERGE (src)-[:ID]->(dst)
RETURN "Created BR->ID links:" AS Result, count(row) AS ` `;

// add resource embodiment links
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-format-link.csv" AS row
MATCH (src:BibliographicResource {iri: row.src})
MATCH (dst:ResourceEmbodiment {iri: row.dst})
MERGE (src)-[:RESOURCE]->(dst)
RETURN "Created BR->RE links:" AS Result, count(row) AS ` `;

// add bibliographic entry reference links
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-reference-link.csv" AS row
MATCH (src:BibliographicResource {iri: row.src})
MATCH (dst:BibliographicEntry {iri: row.dst})
MERGE (src)-[:BIBLOGRAPHY]->(dst)
RETURN "Created BR->BE links:" AS Result, count(row) AS ` `;

// add contributor links and roles
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-contrib-link.csv" AS row
MATCH (br:BibliographicResource {iri: row.src})
MATCH (role:Role {iri: row.dst})
MATCH (agent:Agent {iri: role.role_of})
MERGE (agent)-[:CONTRIBUTOR {r: role.role_type}]->(br)
RETURN "Created BR contributor links:" AS Result, count(row) AS ` `;

// add contributor links
// USING PERIODIC COMMIT 10000
// LOAD CSV WITH HEADERS FROM "file:///br-contrib-link.csv" AS row
// MATCH (br:BibliographicResource {iri: row.src})
// MATCH (role:Role {iri: row.dst})
// MERGE (br)-[:ROLE {role: role.role_type}]->(role);

// add citation links
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///br-citation-link.csv" AS row
MATCH (src:BibliographicResource {iri: row.src})
MATCH (dst:BibliographicResource {iri: row.dst})
MERGE (src)-[:CITATION]->(dst)
RETURN "Created BR citation links:" AS Result, count(row) AS ` `;

RETURN "done." AS Result;
