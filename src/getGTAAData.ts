import { Store, Parser } from "n3";
import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";

// Define the SPARQL endpoint and datasetName
const endpointUrl = "https://gtaa.apis.beeldengeluid.nl/sparql";
const datasetName = "Construct-Thesaurus";

// Define the SPARQL query
const sparqlQuery = `
prefix gtaa: <http://data.beeldengeluid.nl/gtaa/>
prefix skos: <http://www.w3.org/2004/02/skos/core#>
prefix sdo: <http://schema.org/>

construct {
  ?person a sdo:Person ;
          skos:prefLabel ?originalName ;
          skos:exactMatch ?exactMatch .
}
  where {
    ?person skos:inScheme gtaa:Persoonsnamen ;
            skos:prefLabel ?originalName ;
            skos:exactMatch ?exactMatch .
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
      // Set Accept header for Turtle format
      const response = await fetch(queryUrl, {
        headers: {
          'Accept': 'application/n-triples'  // Specify format via Accept header
        }
      });

      // Ensure the response is OK (status code 200)
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      // Get the response as text
      const responseData = await response.text();

      // Parse RDF data (assuming Turtle format)
      const parser = new Parser({ format: "Turtle" });  // Adjusted format to Turtle
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
      defaultGraphName:
        "https://podiumkunst.triply.cc/Personenthesaurus/Personenthesaurus/graphs/gtaa",
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
