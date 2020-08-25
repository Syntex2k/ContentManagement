package runners

import dataModel.DataPaths
import experiments.{Embeddings, IndicatorAnalysis, LocalBodyCleaner, SentimentAnalysis, SmokingIndicator, TopicExtraction}
import org.apache.spark.sql.SparkSession

/**
 * Util function selecting the experiment to start.
 * This allows to generalise the spark-submit call.
 */
object ExperimentSelector {

  def startExperiment(spark: SparkSession, experiment: String, dataPaths: DataPaths)
  : Unit = experiment match {
    case "IndicatorAnalysis" => new IndicatorAnalysis(spark, dataPaths).start()
    case "TopicExtraction" => new TopicExtraction(spark, dataPaths).start()
    case "Embeddings" => new Embeddings(spark, dataPaths).start()
    case "SmokingIndicator" => new SmokingIndicator(spark, dataPaths).start()
    case "SentimentAnalysis" => new SentimentAnalysis(spark, dataPaths).start()
    case "LocalBodyCleaner" => new LocalBodyCleaner(spark, dataPaths).start()
  }
}
