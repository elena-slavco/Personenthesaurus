import { Store, Parser } from "n3";
import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";

// Define the SPARQL endpoint and datasetName
const endpointUrl = "https://data.bibliotheken.nl/sparql";
const accountName = "Personenthesaurus-Acceptance";
const datasetName = "Construct-Thesaurus";
const graphName =
  "https://podiumkunst.triply.cc/Personenthesaurus/Construct-Thesaurus/graphs/kb";

// Define the SPARQL query
const sparqlQuery = `
prefix kb-dataset: <http://data.bibliotheken.nl/id/dataset/> 
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix sdo: <http://schema.org/>

construct {
  ?person a sdo:Person ;
    sdo:name ?name ;
    sdo:birthDate ?birthDate, ?birthdate_DBNL ;
    sdo:deathDate ?deathDate ;
    sdo:alternateName ?alternateName .
  } where {
  ?person sdo:mainEntityOfPage/sdo:isPartOf kb-dataset:persons ;
    sdo:name ?name .
  optional { ?person sdo:birthDate ?birthDate }
  optional { ?person sdo:deathDate ?deathDate }
  optional { ?person sdo:alternateName ?alternateName }
  filter exists {
    ?creativeWork a sdo:CreativeWork ;
    ?relation ?person
  }
  optional {
    ?dbnlaperson owl:sameAs ?person ;
      sdo:mainEntityOfPage/sdo:isPartOf kb-dataset:dbnla ;
      sdo:birthDate ?birthdate_DBNL .
  }
  # Ensure that at least one date is bound
  filter(bound(?birthDate) || bound(?birthdate_DBNL))
}
`;

// Variables for pagination
const limit = 10000;
let offset = 0;

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
        parser.parse(responseData, async (error, quad, prefixes) => {
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
              // graph.addQuads([...tempGraph.getQuads(null, null, null, null)]);
              console.info("Graph size:", tempGraph.size);
              try {
                console.info("Uploading graph to TriplyDB...");
                await dataset.importFromStore(tempGraph, {
                  defaultGraphName: graphName,
                  mergeGraphs: true,
                });
                console.info("Done uploading graph to TriplyDB");
              } catch (uploadError) {
                console.error(
                  "Error uploading graph to TriplyDB:",
                  uploadError,
                );
              }
            }
            resolve();
          }
        });
      });
    } catch (error) {
      console.error("Error fetching data:", error, "Failing offset:", offset);
      shouldContinue = false;
    }

    offset += limit; // Move to the next page
  } while (shouldContinue);
}

// Call the fetchData function to start the process
fetchData().catch((error) => {
  console.error("Error in fetchData:", error);
});
