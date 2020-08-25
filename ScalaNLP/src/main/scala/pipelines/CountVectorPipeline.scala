package pipelines

import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import dataModel.Keys
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StopWordsRemover}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pretrained.ModelLoader

case class VectorizerParameters(vocabularySize: Int, stopWords: Array[String])

class CountVectorPipeline(spark: SparkSession, modelsDirectory: String, vectorizerParams: VectorizerParameters) {
  val debugVecExtraction = true

  // Coverts text into a document format which can be fed into the pipeline.
  private val document = new DocumentAssembler()
    .setInputCol(Keys.text)
    .setOutputCol(Keys.document)

  // Splits the document into sentences.
  private val sentence = new SentenceDetector()
    .setInputCols(Array(Keys.document))
    .setOutputCol(Keys.sentence)

  // Splits the document into tokens (mostly single words).
  private val tokenizer = new Tokenizer()
    .setInputCols(Keys.sentence)
    .setOutputCol(Keys.token)

  // Removes unnecessary characters.
  private val normalizer = new Normalizer()
    .setInputCols(Keys.token)
    .setOutputCol(Keys.normal)

  // Reduces words to the core by removing conjugations etc.
  private val lemmatizer = LemmatizerModel
    .load(ModelLoader.getLemmatizerModelPath(modelsDirectory))
    .setInputCols(Keys.normal)
    .setOutputCol(Keys.lemma)

  private val lemmaFinisher = new Finisher()
    .setInputCols(Keys.lemma)
    .setCleanAnnotations(true)

  // Removes many english auxiliary words which carry no meaning.
  private val stopWordsRemover = new StopWordsRemover()
    .setInputCol(Keys.finishedLemma)
    .setOutputCol(Keys.lemmaTokens)
    .setCaseSensitive(false)
  stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ vectorizerParams.stopWords)

  // Model which builds vectors from text by counting word appearances based on a vocabulary.
  private val countVectorizer = new CountVectorizer()
    .setVocabSize(vectorizerParams.vocabularySize)
    .setInputCol(Keys.lemmaTokens)
    .setOutputCol(Keys.features)

  // Chaining up all the modules to a pipeline.
  private val pipeline: Pipeline = new Pipeline()
    .setStages(Array(
      document,
      sentence,
      tokenizer,
      normalizer,
      lemmatizer,
      lemmaFinisher,
      stopWordsRemover,
      countVectorizer))

  // Count vectorizer stage index 7 used below. Adjust if more stages are added to the pipeline.
  val countVectorizerStageIndex = 7

  /**
   * Build vectors from text data.
   *
   * @return (corpus, vocabulary, total token count in corpus)
   */
  def run(dataFrame: DataFrame)
  : (DataFrame, Array[String]) = {
    // The pipeline has to fit a data frame to become a model for transformation.
    val model = pipeline.fit(dataFrame)
    // The trained vectorizer model can be extracted from the pipeline. It contains the vocabulary.
    val vocabulary = model.stages(countVectorizerStageIndex).asInstanceOf[CountVectorizerModel].vocabulary
    // After running the pipeline we have a vector representation of each paragraph.
    (model.transform(dataFrame), vocabulary)
  }
}
