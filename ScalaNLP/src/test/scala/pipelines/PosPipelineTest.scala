package pipelines

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class PosPipelineTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val posPipeline = new PosPipeline(spark, testModelsFolder)


  test("should add all needed columns in the data frame") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val result = posPipeline.run(dataFrame)

    val columnNames = result.schema.fields.map(x => x.name)
    assert(columnNames === List("paper_id", "paragraph_index", "text", "pos"))
  }
}
