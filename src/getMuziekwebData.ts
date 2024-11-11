import { Store, Parser } from "n3";
import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";

// Define the SPARQL endpoint and datasetName
const endpointUrl =
  "https://api.data.muziekweb.nl/datasets/MuziekwebOrganization/Muziekweb/services/Muziekweb/sparql";
const accountName = "Personenthesaurus-Acceptance";
const datasetName = "Construct-Thesaurus";
const graphName =
  "https://podiumkunst.triply.cc/Personenthesaurus/Construct-Thesaurus/graphs/muziekweb";

// Define the SPARQL query
const sparqlQuery = `
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix schema: <http://schema.org/>
prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix vocab: <https://data.muziekweb.nl/vocab/>

construct {
  ?person a schema:Person ;
          skos:prefLabel ?name ;
          vocab:beginYear ?beginYear ;
          vocab:endYear ?endYear ;
          skos:altLabel ?altName ;
          owl:sameAs ?person1 .
}
  where {
    graph <https://data.muziekweb.nl/MuziekwebOrganization/Muziekweb/graphs/default> {
      ?person a schema:Person;
              skos:prefLabel ?name .
      optional {
        ?person vocab:beginYear ?beginYear
      }
      optional {
        ?person vocab:endYear ?endYear
      }
      optional {
        ?person skos:altLabel ?altName
      }
      optional {
        ?person owl:sameAs ?person1
      }
    }
  }
`;

// Variables for pagination
const limit = 10000;
let offset = 0;

const graph = new Store();

// Loop to fetch all results using pagination
async function fetchData() {
  dotenv.config();
  const triply = App.get({ token: process.env.TRIPLYDB_TOKEN });
  const account = await triply.getAccount(accountName);

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
      // Set Accept header for the desired format
      const response = await fetch(queryUrl, {
        headers: {
          Accept: "application/n-triples", // Specify desired format via Accept header
        },
      });

      // Ensure the response is OK (status code 200)
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Get the response as text
      const responseData = await response.text();

      // Parse RDF data (assuming N-Triples format)
      const parser = new Parser({ format: "N-Triples" });
      const tempGraph = new Store();

      await new Promise<void>((resolve, reject) => {
        parser.parse(responseData, (error, quad, prefixes) => {
          if (error) {
            console.error("Error parsing quad:", error);
            reject(error);
          }
          if (quad) {
            try {
              tempGraph.addQuad(quad);
            } catch (error) {
              console.error("Error adding quad:", error);
            }
          } else {
            if (tempGraph.size === 0) {
              console.info("No more data, stopping pagination.");
              shouldContinue = false;
            } else {
              graph.addQuads([...tempGraph.getQuads(null, null, null, null)]);
              console.info("Graph size:", graph.size);
            }
            resolve();
          }
        });
      });
    } catch (error) {
      console.error("Error fetching data:", error);
      shouldContinue = false;
    }

    offset += limit; // Move to the next page
  } while (shouldContinue);

  try {
    console.info("Uploading graph to TriplyDB...");
    await dataset.importFromStore(graph, {
      defaultGraphName: graphName,
      overwriteAll: true,
    });
    console.info("Done uploading graph to TriplyDB");
  } catch (uploadError) {
    console.error("Error uploading graph to TriplyDB:", uploadError);
  }
}

// Call the fetchData function to start the process
fetchData().catch((error) => {
  console.error("Error in fetchData:", error);
});
