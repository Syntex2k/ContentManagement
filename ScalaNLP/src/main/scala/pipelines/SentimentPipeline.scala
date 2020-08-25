package pipelines

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import dataModel.Keys
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}
import pretrained.ModelLoader

class SentimentPipeline(spark: SparkSession, modelsDirectory: String) {

  val pipeline: PretrainedPipeline = PretrainedPipeline.fromDisk(ModelLoader.getSentimentAnalyzerModelPath(modelsDirectory))

  def run(dataFrame: DataFrame)
  : DataFrame = {
    val df = dataFrame.withColumn(Keys.text, explode(dataFrame(Keys.sentence)))
      .withColumnRenamed(Keys.sentence, "temp")
      .select("temp", Keys.text.concat(".sentence"))
      .withColumnRenamed(Keys.sentence, Keys.text)
    df.printSchema()
    df.show(5)
    val result = pipeline.transform(df)
    result
  }
}
