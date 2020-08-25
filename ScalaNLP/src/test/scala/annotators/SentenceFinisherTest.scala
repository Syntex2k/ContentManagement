package annotators

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import pipelines.PreprocessingPipeline

class SentenceFinisherTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val preprocessingPipeline = new PreprocessingPipeline(spark, inputdataLoader.loadStopWords(), testModelsFolder)


  test("should remove all unnecessary columns") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val result = preprocessingPipeline.run(dataFrame)

    val columnNames = result.schema.fields.map(x => x.name)
    assert(columnNames === List("paper_id", "paragraph_index", "text", "document", "sentence", "token", "normal", "cleanToken", "lemma"))

    val cleanedDataFrame = SentenceFinisher.cleanup(result)
    val cleanedColumnNames = cleanedDataFrame.schema.fields.map(x => x.name)
    cleanedColumnNames.foreach(element => println(element))
    assert(cleanedColumnNames === List("paper_id", "paragraph_index", "text", "document", "sentence", "token", "normal", "cleanToken", "lemma"))
  }
}
