package pipelines

import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import dataModel.Keys
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{EMLDAOptimizer, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pretrained.ModelLoader

case class LdaParameters(k: Int, maxIterations: Int,
                         docConcentration: Double,
                         topicConcentration: Double,
                         checkPointInterval: Int,
                         maxTermsPerTopic: Int)

class LdaPipeline(spark: SparkSession,
                  vectorizerParams: VectorizerParameters,
                  ldaParams: LdaParameters,
                  modelsDirectory: String) {
  val keyLemmaTokens = "lemma_tokens"

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
    .setOutputCol(keyLemmaTokens)
    .setCaseSensitive(false)
  stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ vectorizerParams.stopWords)

  private val countVectorizer = new CountVectorizer()
    .setVocabSize(vectorizerParams.vocabularySize)
    .setInputCol(keyLemmaTokens)
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

  /**
   * Pre-processes text data frames: tokenize them, create vocabulary, and prepare documents as term count vectors.
   *
   * @return (corpus, vocabulary, total token count in corpus)
   */
  def preprocess(dataFrame: DataFrame)
  : (RDD[(Long, Vector)], Array[String], Long) = {
    // The pipeline has to fit a data frame to become a model for transformation.
    val model = pipeline.fit(dataFrame)
    // The trained vectorizer model can be extracted from the pipeline. It contains the vocabulary.
    val vocabulary = model.stages(7).asInstanceOf[CountVectorizerModel].vocabulary
    // After running the pipeline we have a vector representation of each paragraph.
    val corpus = model.transform(dataFrame)
      // The vector representation is stored under features.
      .select(Keys.features).rdd
      // We need to convert new linear algebra type to older ml library type.
      .map { case Row(features: MLVector) => Vectors.fromML(features) }
      // Combine the vector representation with the paragraph (row) index.
      .zipWithIndex().map(tuple => tuple.swap)
    // Finally we can find out how many tokens we have.
    val tokenCount = corpus.map(tuple => tuple._2.numActives).sum.toLong
    (corpus, vocabulary, tokenCount)
  }

  def run(corpus: RDD[(Long, Vector)], vocabulary: Array[String])
  : Array[Array[(String, Double)]] = {
    // Setup hyper-parameters and optimizer for the lda model.
    val lda = new LDA
    val optimizer = new EMLDAOptimizer
    lda.setOptimizer(optimizer)
      .setK(ldaParams.k)
      .setMaxIterations(ldaParams.maxIterations)
      .setDocConcentration(ldaParams.docConcentration)
      .setTopicConcentration(ldaParams.topicConcentration)
      .setCheckpointInterval(ldaParams.checkPointInterval)
    // Train the lda algorithm on the corpus.
    val ldaModel = lda.run(corpus)
    // Return the topics, showing the top-weighted terms for each topic.
    val topicRows = ldaModel.describeTopics(maxTermsPerTopic = ldaParams.maxTermsPerTopic)
    topicRows.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabulary(term.toInt), weight) }
    }
  }
}
