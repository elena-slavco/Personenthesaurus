import { Store } from "n3";
import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";
import { RdfXmlParser } from "rdfxml-streaming-parser";
import { Readable } from "stream";

// Define the SPARQL endpoint and datasetName
const endpointUrl = "https://data.muziekschatten.nl/sparql";
const datasetName = "Construct-Thesaurus";

// Define the SPARQL query
const sparqlQuery = `
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix schema: <http://schema.org/>
prefix som: <https://data.muziekschatten.nl/som/>

construct {
  ?person a schema:Person .
  ?person som:ZKNMFZ ?rol .
  ?person schema:name ?name .
  ?person schema:birthDate ?birthDate .
  ?person schema:deathDate ?deathDate .
  ?person som:GDAT ?birthYear .
  ?person som:SDAT ?deathYear .
  ?person schema:alternateName ?alternateName .
  ?person owl:sameAs ?link .
  } where {
  ?person a schema:Person;
    som:ZKNMFZ ?rol ;
    schema:name ?name .
    optional { ?person schema:birthDate ?birthDate }
    optional { ?person schema:deathDate ?deathDate }
    optional { ?person som:GDAT ?birthYear }
    optional { ?person som:SDAT ?deathYear }
    optional { ?person schema:alternateName ?alternateName }
    optional { ?person owl:sameAs ?link }
}
`;

// Variables for pagination
const limit = 1000;
let offset = 0;

const graph = new Store();

// Loop to fetch all results using pagination
async function fetchData() {
  dotenv.config();
  const triply = App.get({ token: process.env.TRIPLYDB_TOKEN });
  const account = await triply.getAccount('Personenthesaurus');

  let dataset: Dataset;
  try {
    dataset = await account.getDataset(datasetName);
  } catch (error) {
    dataset = await account.addDataset(datasetName);
  }
  if (!dataset)
    throw new Error(`Kon de dataset ${datasetName} niet aanmaken in TriplyDB`);

  let shouldContinue = true;

  do {
    const paginatedQuery = `${sparqlQuery} LIMIT ${limit} OFFSET ${offset}`;
    const encodedQuery = encodeURIComponent(paginatedQuery);
    const queryUrl = `${endpointUrl}?query=${encodedQuery}`;

    try {
      // Set Accept header for RDF/XML format
      const response = await fetch(queryUrl, {
        headers: {
          'Accept': 'application/rdf+xml'  // Specify RDF/XML format via Accept header
        }
      });

      // Ensure the response is OK (status code 200)
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Get the response as text
      const responseData = await response.text();

      // Use rdfxml-streaming-parser to parse RDF/XML
      const input = new Readable({
        read() {
          this.push(responseData);
          this.push(null);
        },
      });

      const tempGraph = new Store();
      const rdfXmlParser = new RdfXmlParser();
      await new Promise<void>((resolve, reject) => {
        rdfXmlParser
          .import(input)
          .on("data", (quad) => {
            try {
              tempGraph.addQuad(quad);
            } catch (error) {
              console.error("Error adding quad:", error);
            }
          })
          .on("error", (error) => {
            console.error("Parsing error:", error);
            // Log the error and resolve to continue the process
            resolve();
          })
          .on("end", () => {
            if (tempGraph.size === 0) {
              console.info("No more data, stopping pagination.");
              shouldContinue = false;
            } else {
              graph.addQuads([...tempGraph.getQuads(null, null, null, null)]);
              console.info("Graph size:", graph.size);
            }
            resolve();
          });
      });
    } catch (error) {
      console.error("Error fetching data:", error);
      shouldContinue = false;
    }
    offset += limit;
  } while (shouldContinue);

  try {
    console.info("Uploading graph to TriplyDB...");
    await dataset.importFromStore(graph, {
      defaultGraphName:
        "https://podiumkunst.triply.cc/Personenthesaurus/Personenthesaurus/graphs/muziekschatten",
      overwriteAll: true,
    });
    console.info("Done uploading graph to TriplyDB");
  } catch (uploadError) {
    console.error("Error uploading graph to TriplyDB:", uploadError);
  }
}

fetchData().catch((error) => {
  console.error("Error in fetchData:", error);
});
