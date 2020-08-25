package pipelines

import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.DataPaths
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class LdaPipelineTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val spark = SparkSession.builder.
    appName("SparkSessionExample").
    master("local[24]").
    getOrCreate

  val testInputFolder = "data/testData/cleanedData"
  val testUtilFolder = "data/utilDictionaries"
  val testModelsFolder = "models"

  val inputdataLoader = new InputDataLoader(DataPaths(testInputFolder, null, testUtilFolder, testModelsFolder))
  val stopWords = inputdataLoader.loadStopWords()
  val vectorizerParams: VectorizerParameters = VectorizerParameters(vocabularySize = 1000, stopWords = stopWords)
  val ldaParams: LdaParameters = LdaParameters(k = 10, maxIterations = 10,
    docConcentration = -1, topicConcentration = -1, checkPointInterval = 10, maxTermsPerTopic = 16)

  val ldaPipeline = new LdaPipeline(spark, vectorizerParams, ldaParams, testModelsFolder)

  test("should calculate the topics on given vocabulary") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)
    val (corpus, vocabulary, tokenCount) = ldaPipeline.preprocess(dataFrame)

    val topics: Array[Array[(String, Double)]] = ldaPipeline.run(corpus, vocabulary)
    val result = topics.flatten.map(x => x._1)
    val expectedResult = Array("structure", "RdRp", "dynamics")
    expectedResult.foreach(element => assert(result.contains(element)))
  }
}
