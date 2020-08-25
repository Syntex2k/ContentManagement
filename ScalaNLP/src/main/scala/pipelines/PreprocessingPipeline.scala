package pipelines

import com.johnsnowlabs.nlp.annotator.{Normalizer, SentenceDetector, StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.annotators.LemmatizerModel
import com.johnsnowlabs.nlp.base.DocumentAssembler
import dataModel.Keys
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import pretrained.ModelLoader

class PreprocessingPipeline(spark: SparkSession, stopWords: Array[String], modelsDirectory: String) {

  import spark.implicits._

  private val document = new DocumentAssembler()
    .setInputCol(Keys.text)
    .setOutputCol(Keys.document)

  private val sentence = new SentenceDetector()
    .setInputCols(Keys.document)
    .setOutputCol(Keys.sentence)

  private val tokenizer = new Tokenizer()
    .setInputCols(Keys.document)
    .setOutputCol(Keys.token)

  private val normalizer = new Normalizer()
    .setInputCols(Keys.token)
    .setOutputCol(Keys.normal)

  private val stopWordsCleaner = new StopWordsCleaner()
    .setInputCols(Keys.token)
    .setOutputCol(Keys.cleanToken)
    .setStopWords(stopWords)
    .setCaseSensitive(false)

  private val lemmatizer = LemmatizerModel
    .load(ModelLoader.getLemmatizerModelPath(modelsDirectory))
    .setInputCols(Keys.cleanToken)
    .setOutputCol(Keys.lemma)

  private val pipeline: Pipeline = new Pipeline()
    .setStages(Array(
      document,
      sentence,
      tokenizer,
      normalizer,
      stopWordsCleaner,
      lemmatizer))

  def run(dataFrame: DataFrame)
  : DataFrame = {
    val model = pipeline.fit(Seq.empty[String].toDS.toDF(Keys.text))
    val result = model.transform(dataFrame)
    result
  }
}
