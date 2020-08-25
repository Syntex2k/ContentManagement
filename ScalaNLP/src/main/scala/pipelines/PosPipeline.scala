package pipelines

import com.johnsnowlabs.nlp.annotator.Tokenizer
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.base.DocumentAssembler
import dataModel.Keys
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import pretrained.ModelLoader

import scala.collection.mutable

class PosPipeline(spark: SparkSession, modelsDirectory: String) {
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  import spark.implicits._

  // Coverts text into a document format which can be fed into the pipeline.
  private val document = new DocumentAssembler()
    .setInputCol(Keys.text)
    .setOutputCol(Keys.document)

  // Splits the document into tokens (mostly single words).
  private val tokenizer = new Tokenizer()
    .setInputCols(Keys.document)
    .setOutputCol(Keys.token)

  // Uses a pre-trained neural network to tag part of speech.
  private val posPerceptron = PerceptronModel
    .load(ModelLoader.getPosPerceptronModelPath(modelsDirectory))
    .setInputCols(Keys.token, Keys.document)
    .setOutputCol(Keys.pos)

  // Chaining up all the modules to a pipeline.
  private val pipeline: Pipeline = new Pipeline()
    .setStages(Array(
      document,
      tokenizer,
      posPerceptron))

  def run(dataFrame: DataFrame)
  : DataFrame = {
    // Fit the pipeline (train) and run it on our data frame.
    val model = pipeline.fit(Seq.empty[String].toDS.toDF(Keys.text))
    // Remove the annotation columns which where added during the pipeline run.
    model.transform(dataFrame).drop(Keys.token, Keys.document)
  }
}
