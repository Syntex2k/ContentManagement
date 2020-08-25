package pipelines

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * This pipeline is used to get the context of the paragraph in the text
 */
object ContentPipeline {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("TopicAnalyzer")
    .master("local[*]")
    .config("spark.driver.memory", "12G")
    .getOrCreate()

  import spark.implicits._

  private val pipeline = PretrainedPipeline("explain_document_dl", lang = "en")

  def getContentOfText(dataFrame: DataFrame): DataFrame = {
    val rdd = dataFrame.withColumn("content", dataFrame("text")).rdd
      .map(x => (x.getAs[String]("text"), x.getAs[String]("content")))
      .map(x => {
        val text = pipeline.annotate(x._2)
        text.get("entities") match {
          case Some(value) => (x._1, value.toList)
          case None => (x._1, List("Empty"))
        }
      })
    rdd.toDF("text", "content");
  }
}
