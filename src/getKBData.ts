import { Store, Parser } from "n3";
import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";

// Define the SPARQL endpoint and datasetName
const endpointUrl = "https://data.bibliotheken.nl/sparql";
const accountName = "PT";
const datasetName = "Construct-Thesaurus";

// Define the SPARQL query
const sparqlQuery = `
prefix kb-dataset: <http://data.bibliotheken.nl/id/dataset/> 
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix schema: <http://schema.org/>

construct {
  ?person a schema:Person ;
    schema:name ?name ;
    rdfs:label ?name ;
    schema:birthDate ?birthDate ;
    schema:deathDate ?deathDate ;
    schema:alternateName ?alternateName ;
     owl:sameAs ?link .
  } where {
  ?creativeWork a schema:CreativeWork ;
    ?relation ?person .
  # ?creativeWork rdfs:label ?label . # het label van een CreativeWork bestaat uit titel / verantwoordelijkheidsvermelding
  graph <http://data.bibliotheken.nl/persons/2023-r02/> {
    ?person schema:mainEntityOfPage/schema:isPartOf kb-dataset:persons ; # NTA 
      schema:name ?name .
    ?person rdfs:label ?label_person_NTA .
    optional { ?person schema:birthDate ?birthDate }
    optional { ?person schema:deathDate ?deathDate }
    optional { ?person schema:alternateName ?alternateName }
    optional { ?person owl:sameAs ?link }
  }
}
`;

// Variables for pagination
const limit = 10000;
let offset = 0;

let graph = new Store();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
      console.error("Error fetching data: ", error);
      console.error("Last offset: ", offset);
      shouldContinue = false;
    }

    if (shouldContinue) {
      try {
        console.info("Uploading graph to TriplyDB...");
        await dataset.importFromStore(graph, {
          defaultGraphName:
            "https://podiumkunst.triply.cc/Personenthesaurus/Construct-Thesaurus/graphs/nta",
          mergeGraphs: true,
        });
        // Clear the graph after uploading
        graph = new Store();
        console.info("Done uploading graph to TriplyDB");
      } catch (uploadError) {
        console.error("Error uploading graph to TriplyDB:", uploadError);
      }
    }

    // await sleep(2000); // sleep to work around endpoint performance
    offset += limit; // Move to the next page
  } while (shouldContinue);
}

// Call the fetchData function to start the process
fetchData().catch((error) => {
  console.error("Error in fetchData:", error);
});
