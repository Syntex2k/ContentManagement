package pipelines
import com.johnsnowlabs.nlp.annotator.{BertEmbeddings, Normalizer, SentenceDetector, StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.johnsnowlabs.nlp.annotators.LemmatizerModel
import com.johnsnowlabs.nlp.annotators.classifier.dl.ClassifierDLApproach
import com.johnsnowlabs.nlp.embeddings.{SentenceEmbeddings, WordEmbeddings, WordEmbeddingsModel}
import dataModel.Keys
import pretrained.ModelLoader

class EmbeddingsPipeline(spark: SparkSession, stopWords: Array[String], modelsDirectory: String) {
  import spark.implicits._

  private val documentAssembler = new DocumentAssembler()
    .setInputCol(Keys.text)
    .setOutputCol(Keys.document)

  private val tokenizer = new Tokenizer()
    .setInputCols(Keys.document)
    .setOutputCol(Keys.token)

  private val normalizer = new Normalizer()
    .setInputCols(Keys.token)
    .setOutputCol(Keys.normal)

  private val stopWordsCleaner = new StopWordsCleaner()
    .setInputCols(Keys.normal)
    .setOutputCol(Keys.cleanToken)
    .setStopWords(stopWords)
    .setCaseSensitive(false)

  private val lemmatizer = LemmatizerModel
    .load(ModelLoader.getLemmatizerModelPath(modelsDirectory))
    .setInputCols(Keys.cleanToken)
    .setOutputCol(Keys.lemma)

  val bertEmbeddings = BertEmbeddings
    .load(ModelLoader.getBertModelPath(modelsDirectory))
    .setInputCols(Keys.document,Keys.lemma)
    .setOutputCol(Keys.wordEmbeddings)
    .setCaseSensitive(false)

  private val sentenceEmbeddings = new SentenceEmbeddings()
    .setInputCols(Keys.document, Keys.wordEmbeddings)
    .setOutputCol(Keys.sentenceEmbeddings)
    .setPoolingStrategy("AVERAGE")

  private val clfPipeline: Pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      tokenizer,
      normalizer,
      stopWordsCleaner,
      lemmatizer,
      bertEmbeddings,
      sentenceEmbeddings))

  def run(dataFrame: DataFrame)
  : DataFrame = {
    val model = clfPipeline.fit(Seq.empty[String].toDS.toDF(Keys.text))
    val result = model.transform(dataFrame)
    result
  }
}
