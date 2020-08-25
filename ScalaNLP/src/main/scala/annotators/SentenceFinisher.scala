package annotators

import dataModel.{CleanSentenceEntry, Keys, TextCoordinate}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object SentenceFinisher {
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  /**
   * Cleans up the sentence annotation in a data frame.
   * We want just the essential information [enumeratingIndexInParagraph, beginInParagraph, endInParagraph].
   * All other information is already included in other data frame columns and would be doubled up.
   *
   * @param dataFrame to clean up (in most case obtained from the first nlp pipeline)
   * @return data frame with leaner data entries for sentences
   */
  def cleanup(dataFrame: DataFrame)
  : DataFrame = {
    dataFrame.withColumn(Keys.sentence, reformatSentenceEntriesUdf(dataFrame(Keys.sentence)))
  }

  private val reformatSentenceEntriesUdf = udf((sentences: StructArray) => sentences
    // Extract just the essential information from the sentence annotation. These are the coordinates in the paragraph.
    .map(elem => TextCoordinate(elem.getAs[Int](Keys.begin), elem.getAs[Int](Keys.end)))
    // Sort by coordinate in ascending order. This allows us to index the sentences in order of appearance.
    .sortWith((textCoordinateA, textCoordinateB) => textCoordinateA.begin < textCoordinateB.begin)
    // Indexing can be done by a special zip function and some repackaging into a clean sentence object.
    .zipWithIndex.map(textCoord => CleanSentenceEntry(textCoord._2, textCoord._1.begin, textCoord._1.end))
  )
}
