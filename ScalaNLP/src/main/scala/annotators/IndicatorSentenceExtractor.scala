package annotators

import dataModel.{IndicatorSentenceEntry, IndicatorWordEntry, Keys, SimilarSentenceEntry}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.collection.mutable

class IndicatorSentenceExtractor(spark: SparkSession) {
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  def extract(dataFrame: DataFrame, indicator: Indicator)
  : DataFrame = {
    // From ech paragraph (row) extract just the sentences which are holding indicator words.
    val indicatorWordsColumn = IndicatorAnnotator.getIndicatorWordsColumnName(indicator)
    val extendedDf = dataFrame
      .withColumn(Keys.sentence, sentenceExtractionUdf(
        dataFrame(Keys.paperId), dataFrame(Keys.paragraphIndex), dataFrame(Keys.text),
        dataFrame(indicatorWordsColumn), dataFrame(Keys.sentence)))
      .select(Seq(Keys.sentence).map(c => col(c)): _*)
    // As we might have multiple indicator sentences in one paragraph, we have arrays in one row.
    // We want to give each sentence its own row, which can be done via explode.
    extendedDf.withColumn(Keys.sentence, explode(extendedDf(Keys.sentence)))
  }

  private val sentenceExtractionUdf = udf(
    (paperId: String, paragraphIndex: Long, text: String, indicators: StructArray, sentences: StructArray) => {
      sentences.map(sentence => {
        val sentenceBegin = sentence.getAs[Int](Keys.begin)
        val sentenceEnd = sentence.getAs[Int](Keys.end)
        val sentenceIndex = sentence.getAs[Int](Keys.index)
        val sentenceText = text.substring(sentenceBegin, sentenceEnd + 1)
        val indicatorsInSentence = indicators
          // Filter out the indicator words belonging into the one sentence. They share the same sentence begin.
          .filter(indicator => sentenceBegin.equals(indicator.getAs[Int](Keys.sentenceBegin)))
          // Now map the reduced selection to proper indicator entries.
          // See that the text coordinate indices are recalculated to be relative to the sentence.
          .map(indicator => IndicatorWordEntry(
            indicator.getAs[String](Keys.word),
            indicator.getAs[Int](Keys.begin) - sentenceBegin,
            indicator.getAs[Int](Keys.end) - sentenceBegin))
          .toList
        // Create a new indicator sentence entry.
        IndicatorSentenceEntry(
          paperId,
          paragraphIndex,
          sentenceIndex,
          sentenceText,
          sentenceBegin,
          sentenceEnd,
          indicatorsInSentence,
          List[SimilarSentenceEntry](),
          0.0)
      }).filter(sentence => sentence.indicators.nonEmpty)
    }
  )
}

object SentenceUtils {

  def unfoldSentencesDataFrame(dataFrame: DataFrame, sourceColumn: String, outputColumns: Seq[String])
  : DataFrame = {
    @tailrec
    def spreadAttributesAsColumns(acc: DataFrame, dataAttributes: Seq[String])
    : DataFrame = dataAttributes match {
      case Nil => acc
      case x :: xs => spreadAttributesAsColumns(acc.withColumn(x, dataFrame(sourceColumn + "." + x)), xs)
    }
    // Spread all data attributes of the sentence entry as columns and finally drop the original sentence column.
    spreadAttributesAsColumns(dataFrame, outputColumns).drop(sourceColumn)
  }
}
