import App from "@triply/triplydb";
import dotenv from "dotenv";

// Define constants
const graphs =
  "https://podiumkunst.triply.cc/Personenthesaurus/Construct-Thesaurus/graphs/";
const verrijkingGraphName = graphs + "verrijkingen";
const relatiesGraphName = graphs + "relaties";
const coreGraphName = graphs + "core";

dotenv.config();
const triply = App.get({ token: process.env.TRIPLYDB_TOKEN });

async function deleteGraph(dataset: any, graphName: string): Promise<void> {
  try {
    const graph = await dataset.getGraph(graphName);
    await graph.delete();
  } catch (error) {}
}

async function runPipeline(
  account: any,
  queries: any[],
  dataset: any,
  graphName: string,
): Promise<void> {
  try {
    await account.runPipeline({
      queries: queries,
      destination: {
        dataset: dataset,
        graph: graphName,
      },
    });
  } catch (error) {
    console.error(`Error running pipeline for graph ${graphName}:`, error);
  }
}

async function runPipelines(): Promise<void> {
  const account = await triply.getAccount("Personenthesaurus");

  // Get the datasets
  const constructThesaurusDataset = await account.getDataset(
    "Construct-Thesaurus",
  );
  const thesaurusDataset = await account.getDataset("Thesaurus");

  // Get the queries
  const wikidata = await account.getQuery("muziekweb-wikidata-fix");
  const ptcallSigns = await account.getQuery("pt-callSigns");
  const ptRelations = await account.getQuery("pt-relations");
  const thesaurusCore = await account.getQuery("thesaurus-core");

  console.info("Delete existing graphs");
  await deleteGraph(constructThesaurusDataset, verrijkingGraphName);
  await deleteGraph(constructThesaurusDataset, relatiesGraphName);
  await deleteGraph(constructThesaurusDataset, coreGraphName);
  await deleteGraph(thesaurusDataset, coreGraphName);

  console.info("Verrijkingen: muziekweb-wikidata-fix, pt-callSigns");
  await runPipeline(
    account,
    [wikidata, ptcallSigns],
    constructThesaurusDataset,
    verrijkingGraphName,
  );

  console.info("Relaties: pt-relations");
  await runPipeline(
    account,
    [ptRelations],
    constructThesaurusDataset,
    relatiesGraphName,
  );

  console.info("Thesaurus Core => Thesaurus");
  await runPipeline(account, [thesaurusCore], thesaurusDataset, coreGraphName);

  console.info("Thesaurus Core => Construct Thesaurus");
  await runPipeline(
    account,
    [thesaurusCore],
    constructThesaurusDataset,
    coreGraphName,
  );
}

// Call the runPipelines function to start the process
runPipelines().catch((error) => {
  console.error("Error in runPipelines:", error);
});
