package experiments

import dataIO.JsonFileUtils
import dataModel.{DataPaths, InputKeys, Keys}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipelines.{LdaParameters, LdaPipeline, VectorizerParameters}

class TopicExtraction(spark: SparkSession, dataPaths: DataPaths) {
  val debug = true
  val debugStopWords = true

  def start()
  : Unit = {
    // Load in the cleaned text data we want to process.
    val inputColumns = Seq(InputKeys.paperId, InputKeys.paragraphIndex, Keys.text)
    val inputDataFrame = JsonFileUtils.readInputJsonToDataFrame(dataPaths.input, inputColumns)
    // Load in a dictionary of english words which carry less meaning.
    val stopWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.stopWords) ++
      dataIO.JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.coronaStopWords)

    if (debugStopWords) {
      println("Stop Words:")
      stopWords.foreach(word => print(s"$word | "))
      println()
    }
    analyseTopics(inputDataFrame, stopWords)
  }

  private def analyseTopics(inputDataFrame: DataFrame, stopWords: Array[String])
  : Unit = {
    // Setup the hyper parameters for our training processes. This could be linked to a config file.
    val vectorizerParams: VectorizerParameters = VectorizerParameters(vocabularySize = 1000, stopWords = stopWords)
    val ldaParams: LdaParameters = LdaParameters(k = 10, maxIterations = 10,
      docConcentration = -1, topicConcentration = -1, checkPointInterval = 10, maxTermsPerTopic = 16)

    // Build the lda pipeline pipeline and run the pre-processing steps.
    val ldaPipeline = new LdaPipeline(spark, vectorizerParams, ldaParams, dataPaths.models)
    val (corpus, vocabulary, tokenCount) = ldaPipeline.preprocess(inputDataFrame)
    // Cache the RDD data to avoid multiple read operations if possible.
    corpus.cache()
    val trainingSetSize = corpus.count()
    val vocabularySize = vocabulary.length
    if (debugStopWords) {
      println(s"Vectorizer -> Training Sets: $trainingSetSize")
      println(s"Vectorizer -> Total Tokens: $tokenCount")
      println(s"Vectorizer -> Vocabulary: $vocabularySize")
      vocabulary.zipWithIndex.foreach(indexedWord => print("\"" + indexedWord._1 + "\"" + ", "))
      println("\nVectorizer -> Some Paragraph Vectors:")
      println(corpus.take(5).foreach(paragraphVector => println(paragraphVector)))
    }
    // We need a folder to store the lda training checkpoints.
    val checkpointFolder = "ldaOutput/Checkpoints"
    spark.sparkContext.setCheckpointDir(checkpointFolder)
    // Now run the lda pipeline main step.
    val topics: Array[Array[(String, Double)]] = ldaPipeline.run(corpus, vocabulary)
    if (debug) {
      println(s"Found ${ldaParams.k} Topics:")
      topics.zipWithIndex.foreach { case (topic, index) =>
        println(s"-> Topic $index")
        topic.foreach { case (term, weight) => print(s"$term:${weight.formatted("%1.3f")} | ") }
        println()
      }
    }
  }
}
