package pipelines

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CountVectorPipelineTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val stopWords = inputdataLoader.loadStopWords() ++ inputdataLoader.loadCoronaStopWords()
  val vocabularySize = 1000
  val vectorizerParams: VectorizerParameters = VectorizerParameters(vocabularySize, stopWords)

  val countVectorPipeline = new CountVectorPipeline(spark, testModelsFolder, vectorizerParams)


  test("should add all needed columns in the data frame and filter through the correct vocabulary") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val (resultDf, vocabulary) = countVectorPipeline.run(dataFrame)

    val columnNames = resultDf.schema.fields.map(x => x.name)
    assert(columnNames === Array("paper_id", "paragraph_index", "text", "finished_lemma", "lemmaTokens", "features"))

    val expectedVocabulary = Array("RdRp", "residue", "structure", "FCoV", "PV", "channel", "sequence", "motif", "RNA", "simulation", "dynamics", "thumb", "motion", "analysis", "region", "cat")
    assert(expectedVocabulary.forall(element => vocabulary.contains(element)))
  }
}
