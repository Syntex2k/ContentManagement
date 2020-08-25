package annotators

import dataModel.{CleanLemmaEntry, Keys}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object LemmaFinisher {
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  /**
   * Cleans up the lemma annotation in a data frame.
   * We want just the essential information [lemmaWord, beginInParagraph, endInParagraph].
   * All other information is not really required for our purposes.
   *
   * @param dataFrame to clean up (in most case obtained from the first nlp pipeline)
   * @return data frame with leaner data entries for lemmas
   */
  def cleanup(dataFrame: DataFrame)
  : DataFrame = {
    dataFrame.withColumn(Keys.lemma, removeNonWordLemmasUdf(dataFrame(Keys.lemma)))
  }

  private val removeNonWordLemmasUdf = udf((lemmas: StructArray) => lemmas
    // Extract just the lemma word and the coordinate indices in the original text.
    // These coordinates can help us to extract the original word the lemma refers to.
    .map(elem => CleanLemmaEntry(
      elem.getAs[String](Keys.result), elem.getAs[Int](Keys.begin), elem.getAs[Int](Keys.end)))
    // We also use the chance to remove some lemma entries which are not words.
    .filter(entry => entry.word.matches("\\w+") && entry.word.matches("\\D+"))
  )
}
