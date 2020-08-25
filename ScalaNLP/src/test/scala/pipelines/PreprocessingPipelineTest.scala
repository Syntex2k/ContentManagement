package pipelines

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class PreprocessingPipelineTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val preprocessingPipeline = new PreprocessingPipeline(spark, inputdataLoader.loadStopWords(), testModelsFolder)


  test("should add all needed columns in the data frame") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val result = preprocessingPipeline.run(dataFrame)

    val columnNames = result.schema.fields.map(x => x.name)
    assert(columnNames === List("paper_id", "paragraph_index", "text", "document", "sentence", "token", "normal", "cleanToken", "lemma"))
  }
}
