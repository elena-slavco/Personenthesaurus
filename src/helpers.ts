// Define constants
export const accountName = "Personenthesaurus-Acceptance";
export const constructThesaurusDatasetName = "Construct-Thesaurus";
export const thesaurusDatasetName = "Thesaurus";

export const graphs =
  "https://podiumkunst.triply.cc/Personenthesaurus/Construct-Thesaurus/graphs/";
export const verrijkingGraphName = graphs + "verrijkingen";
export const relatiesGraphName = graphs + "relaties";
export const coreGraphName = graphs + "thesaurus-core";
export const remainingGraphName = graphs + "thesaurus-remaining";
export const thesaurusVerrijkingGraphName = graphs + "thesaurus-verrijking";
export const kbGraphName = graphs + "kb";
export const coreKbGraphName = graphs + "thesaurus-core-kb";

export async function runPipeline(
  account: any,
  queries: any[],
  sourceDataSet: any,
  destinationDataSet: any,
  graphName: string,
): Promise<void> {
  try {
    await account.runPipeline({
      queries: queries,
      destination: {
        dataset: destinationDataSet,
        graph: graphName,
      },
      source: sourceDataSet,
    });
  } catch (error) {
    console.error(`Error running pipeline for graph ${graphName}:`, error);
  }
}
