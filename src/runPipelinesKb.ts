import App from "@triply/triplydb";
import Dataset from "@triply/triplydb/Dataset.js";
import dotenv from "dotenv";
import {
  accountName,
  constructThesaurusDatasetName,
  coreKbGraphName,
  runPipeline,
  thesaurusDatasetName,
  thesaurusVerrijkingGraphName,
} from "./helpers.js";

dotenv.config();
const triply = App.get({ token: process.env.TRIPLYDB_TOKEN });

async function runPipelines(): Promise<void> {
  const account = await triply.getAccount(accountName);

  // Get the datasets
  let constructThesaurusDataset: Dataset;
  try {
    constructThesaurusDataset = await account.getDataset(
      constructThesaurusDatasetName,
    );
  } catch (error) {
    constructThesaurusDataset = await account.addDataset(
      constructThesaurusDatasetName,
    );
  }
  if (!constructThesaurusDataset)
    throw new Error(
      `Kon de dataset ${constructThesaurusDatasetName} niet aanmaken in TriplyDB`,
    );

  let thesaurusDataset: Dataset;
  try {
    thesaurusDataset = await account.getDataset(thesaurusDatasetName);
  } catch (error) {
    thesaurusDataset = await account.addDataset(thesaurusDatasetName);
  }
  if (!thesaurusDataset)
    throw new Error(
      `Kon de dataset ${thesaurusDatasetName} niet aanmaken in TriplyDB`,
    );

  // Get the queries
  const thesaurusCoreKb = await (
    await account.getQuery("thesaurus-core-kb")
  ).useVersion("latest");
  const thesaurusVerrijking = await (
    await account.getQuery("thesaurus-verrijking")
  ).useVersion("latest");

  console.info("Thesaurus Core KB => Construct Thesaurus");
  await runPipeline(
    account,
    [thesaurusCoreKb],
    constructThesaurusDataset,
    constructThesaurusDataset,
    coreKbGraphName,
  );

  console.info("Thesaurus Core KB => Thesaurus");
  await thesaurusDataset.importFromDataset(constructThesaurusDataset, {
    graphNames: [coreKbGraphName],
    overwrite: true,
  });

  console.info("Thesaurus Verrijking => Thesaurus");
  await runPipeline(
    account,
    [thesaurusVerrijking],
    constructThesaurusDataset,
    thesaurusDataset,
    thesaurusVerrijkingGraphName,
  );
}

// Call the runPipelines function to start the process
runPipelines().catch((error) => {
  console.error("Error in runPipelines:", error);
});
