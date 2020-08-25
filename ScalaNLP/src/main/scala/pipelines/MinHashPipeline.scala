package pipelines

import annotators.SentenceUtils
import dataModel.{Keys, SimilarSentenceEntry}
import logging.Logger
import org.apache.spark.ml.feature.{MinHashLSH, MinHashLSHModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MinHashParameters(numberOfHashTables: Int,
                             similarityThreshold: Double,
                             vocabularySize: Int,
                             vocabulary: Array[String])

class MinHashPipeline(spark: SparkSession, minHashParams: MinHashParameters, vectorizedDf: DataFrame) {
  val keyDatasetA = "datasetA"
  val keyDatasetB = "datasetB"
  val keyDistance = "distCol"
  val debugHashed = true
  val debugJoined = true

  private val minHashModel = new MinHashLSH()
    .setInputCol(Keys.features)
    .setOutputCol(Keys.hash)
    .setNumHashTables(minHashParams.numberOfHashTables)
    .fit(vectorizedDf)

  def run(vectorizedDf: DataFrame)
  : DataFrame = {
    val hashedDf = minHashModel.transform(vectorizedDf)
    if (debugHashed) Logger.printDataFrameForDebug(
      "MinHash Annotated:", hashedDf, 5, 20)
    val joinedDf = minHashModel.approxSimilarityJoin(hashedDf, hashedDf, minHashParams.similarityThreshold).toDF
    if (debugJoined) Logger.printDataFrameForDebug(
      "Joined Approximate Similarity:", joinedDf, 5, 80)
    cleanup(joinedDf)
  }

  private def cleanup(dataFrame: DataFrame)
  : DataFrame = {
    val tmpDf = dataFrame
      .withColumn(Keys.similar, similarSentenceExtraction(dataFrame(keyDatasetB), dataFrame(keyDistance)))
      .drop(keyDatasetB)
    val unfoldedColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.sentenceIndex,
      Keys.text, Keys.begin, Keys.end, Keys.pos, Keys.indicators, Keys.sentimentScore)
    SentenceUtils.unfoldSentencesDataFrame(tmpDf, keyDatasetA, unfoldedColumns)
      .drop(keyDistance)
  }

  private val similarSentenceExtraction = udf((sentence: GenericRowWithSchema, distance: Double) => {
    val paperId = sentence.getAs[String](Keys.paperId)
    val paragraphIndex = sentence.getAs[Long](Keys.paragraphIndex)
    val sentenceIndex = sentence.getAs[Int](Keys.sentenceIndex)
    List[SimilarSentenceEntry](SimilarSentenceEntry(paperId, paragraphIndex, sentenceIndex, distance))
  })

  def findSentenceNeighbours(sentenceFeatures: Vector, hashedDf: DataFrame, model: MinHashLSHModel, k: Int = 5)
  : Array[SimilarSentenceEntry] = {
    val neighbourColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.sentenceIndex, keyDistance)
    model.approxNearestNeighbors(hashedDf, sentenceFeatures, k)
      .select(neighbourColumns.map(c => col(c)): _*)
      .collect()
      .map(row => SimilarSentenceEntry(
        row.getAs[String](Keys.paperId),
        row.getAs[Int](Keys.paragraphIndex),
        row.getAs(Keys.sentenceIndex),
        row.getAs[Double](keyDistance)))
  }
}
