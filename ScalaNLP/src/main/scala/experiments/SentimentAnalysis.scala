package experiments

import dataIO.JsonFileUtils
import dataModel.{DataPaths, Keys}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipelines.SentimentPipeline

class SentimentAnalysis(spark: SparkSession, dataPaths: DataPaths) {

  private val debugInput = true

  def start()
  : Unit = {
    // Load in the cleaned text data we want to process.
    val inputColumns = Seq(Keys.sentence)
    val inputDataFrame = JsonFileUtils.readInputJsonToDataFrame(dataPaths.input, inputColumns)

    if (debugInput) {
      inputDataFrame.show(5)
      inputDataFrame.printSchema()
    }
    analyseSentiments(inputDataFrame)
  }

  private def analyseSentiments(inputDataFrame: DataFrame)
  : Unit = {
    val pipeline = new SentimentPipeline(spark, dataPaths.models);
    val result = pipeline.run(inputDataFrame)
    result.show(5)
    result.printSchema()

    result
      .selectExpr("text", "explode(sentiment) sentiments", "text")
      .selectExpr("text", "sentiments.result result", "text")
      .createOrReplaceTempView("result_tbl_")

    val output = spark.sql(
      """
    SELECT
        text,
        CASE WHEN result_tbl_.result>0 THEN 'positive'
        WHEN result_tbl_.result<0 THEN 'negative'
        ELSE 'neutral'
        END AS label,
        result
    FROM
    result_tbl_""")

    output.show(5)
    output.printSchema()

    JsonFileUtils.saveAsJson(output, dataPaths.output + "/" + "sentiments")
  }
}