package annotators

import dataIO.InputDataLoader
import dataModel.{DataPaths, Keys}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import pipelines.PreprocessingPipeline

class IndicatorSentenceExtractorTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val indicatorSentenceExtractor = new IndicatorSentenceExtractor(spark)
  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val preprocessingPipeline = new PreprocessingPipeline(spark, inputdataLoader.loadStopWords(), testModelsFolder)

  val smokingWords = inputdataLoader.loadSmokingWords()
  val smokingIndicator = Indicator("smoking", smokingWords)

  test("should remove all other columns and return only the sentence column") {
    val dataFrame = inputdataLoader.loadInputDataFrame()
    val cleanedDf = preprocessingPipeline.run(dataFrame)
      .withColumn("smokingIndicatorWords", lit(1))

    val result = indicatorSentenceExtractor.extract(cleanedDf, smokingIndicator)
    val resultColumns = result.schema.fields.map(element => element.name)
    assert(resultColumns === List("sentence")) //only sentence column
  }

  test("should correctly unfold indicator sentences and join back to original dataframe") {
    val dataFrame = inputdataLoader.loadInputDataFrame()
    val cleanedDf = preprocessingPipeline.run(dataFrame)
      .withColumn("smokingIndicatorWords", lit(1))

    val result = indicatorSentenceExtractor.extract(cleanedDf, smokingIndicator)
    val resultColumns = result.schema.fields.map(element => element.name)
    assert(resultColumns === List("sentence"))

    val unfoldedColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.sentenceIndex,
      Keys.text, Keys.begin, Keys.end, Keys.indicators, Keys.similar, Keys.sentimentScore)
    val unfoldedResult = SentenceUtils.unfoldSentencesDataFrame(result, "sentence", unfoldedColumns)
    val unfoldedResultColumns = unfoldedResult.schema.fields.map(element => element.name)
    assert(unfoldedResultColumns === Array("paperId", "paragraphIndex", "sentenceIndex", "text", "begin", "end", "indicators", "similar", "sentimentScore"))
  }
}
