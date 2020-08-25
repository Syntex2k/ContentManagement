package annotators

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import pipelines.PreprocessingPipeline

class IndicatorAnnotatorTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val preprocessingPipeline = new PreprocessingPipeline(spark, inputdataLoader.loadStopWords(), testModelsFolder)

  val smokingWords = inputdataLoader.loadSmokingWords()
  val smokingIndicator = Indicator("smoking", smokingWords)

  test("should annotate all indicators") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val result = preprocessingPipeline.run(dataFrame)

    val columnNames = result.schema.fields.map(x => x.name)
    assert(columnNames === List("paper_id", "paragraph_index", "text", "document", "sentence", "token", "normal", "cleanToken", "lemma"))

    val indicatorDf = IndicatorAnnotator.annotate(result, List(smokingIndicator))
    val columnNamesAfterAnnotation = indicatorDf.schema.fields.map(x => x.name)
    assert(columnNamesAfterAnnotation === Array("paper_id", "paragraph_index", "text", "document", "sentence", "token", "normal", "cleanToken", "lemma", "smokingIndicatorWords", "smoking"))
  }

  test("should return the correct flag column name") {
    val result = IndicatorAnnotator.getIndicatorFlagColumnName(smokingIndicator)
    assert(result === "smoking")
  }

  test("should return the correct words column name") {
    val result = IndicatorAnnotator.getIndicatorWordsColumnName(smokingIndicator)
    assert(result === "smokingIndicatorWords")
  }
}
