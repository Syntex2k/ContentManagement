package annotators

import dataModel.{Keys, PosWordEntry}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object PosFinisher {
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  /**
   * Cleans up the pos annotation in a data frame.
   * We want just the essential information and also inject the original word from the paragraph.
   *
   * @param dataFrame to clean up (in most case obtained from a nlp pipeline)
   * @return data frame with leaner data entries for part of speech
   */
  def cleanup(dataFrame: DataFrame)
  : DataFrame = {
    // Remove the annotation column which where added during the pipeline run.
    val cleanDf = dataFrame.drop(Keys.token, Keys.document)
    // Restructure the pos column and include the original word by extracting it from the text.
    cleanDf.withColumn(Keys.pos, posWordExtractionUdf(cleanDf(Keys.pos), cleanDf(Keys.text)))
  }

  /**
   * This udf is required to extract just the necessary pos information.
   * We also want to add in the original word by extracting it from the sentence text.
   */
  private val posWordExtractionUdf = udf((pos: StructArray, text: String) => pos
    .map(elem => {
      val begin = elem.getAs[Int](Keys.begin)
      val end = elem.getAs[Int](Keys.end)
      val tag = elem.getAs[String](Keys.result)
      val word = text.substring(begin, end + 1)
      PosWordEntry(word, tag, begin, end)
    })
    .filterNot(element => element.word.matches("\\W+"))
  )
}
