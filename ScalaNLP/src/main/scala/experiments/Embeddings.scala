package experiments

import convenience.EmbeddingsStarter
import dataIO.JsonFileUtils
import dataModel.{DataPaths, InputKeys, Keys}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipelines.EmbeddingsPipeline

class Embeddings(spark: SparkSession, dataPaths: DataPaths) {
  private val debugStopWords = true
  private val debugSmokingWords = true
  private val debugPipeline = true

  def start()
  : Unit = {
    // Load in the cleaned text data we want to process.
    val inputColumns = Seq(InputKeys.paperId, InputKeys.paragraphIndex, Keys.text)
    val inputDataFrame = JsonFileUtils.readInputJsonToDataFrame(dataPaths.input, inputColumns)

    // Load in a dictionary of english words which act as noise in the data.
    val stopWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.stopWords)
    if (debugStopWords) printWordsForDebug("Stop Words:", stopWords)

    // Load in a custom dictionary of words which are connected to smoking.
    val smokingWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.smokingWords)
    if (debugSmokingWords) printWordsForDebug("Smoking Words:", smokingWords)

    analiseIndicators(inputDataFrame, stopWords, smokingWords)
  }

  private def analiseIndicators(inputDataFrame: DataFrame,
                                stopWords: Array[String],
                                smokingWords: Array[String])
  : Unit = {
    // PIPELINE
    // Build the ml pipeline and run it.
    val pipeline = new EmbeddingsPipeline(spark, stopWords, dataPaths.models)
    val pipelineResultDf = pipeline.run(inputDataFrame)
    if (debugPipeline) printDataFrameForDebug("Pipeline Result:", pipelineResultDf, 5, 20)

    JsonFileUtils.saveAsJson(pipelineResultDf, EmbeddingsStarter.outputDataPath)
  }

  private def printWordsForDebug(headline: String, words: Array[String])
  : Unit = {
    println("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    println(headline)
    words.foreach(word => print(s"$word | "))
    println()
  }

  private def printDataFrameForDebug(headline: String, dataFrame: DataFrame, numRows: Int, truncate: Int)
  : Unit = {
    println("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    println(headline)
    dataFrame.show(5, truncate = truncate)
    dataFrame.printSchema()
  }
}
