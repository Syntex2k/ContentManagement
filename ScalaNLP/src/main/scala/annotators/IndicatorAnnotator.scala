package annotators

import dataModel.{IndicatorParagraphEntry, Keys}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.collection.mutable

case class Indicator(name: String, words: Array[String])

object IndicatorAnnotator {
  val keyDictionary = "indicatorDictionary"
  val keyWordsSuffix = "IndicatorWords"

  type StringArray = mutable.WrappedArray[String]
  type StructArray = mutable.WrappedArray[GenericRowWithSchema]

  def annotate(dataFrame: DataFrame, indicators: List[Indicator])
  : DataFrame = {
    addAllIndicatorColumns(dataFrame, indicators)
  }

  def getIndicatorFlagColumnName(indicator: Indicator): String = indicator.name

  def getIndicatorWordsColumnName(indicator: Indicator): String = indicator.name + keyWordsSuffix

  @scala.annotation.tailrec
  private def addAllIndicatorColumns(dataFrame: DataFrame, indicators: List[Indicator])
  : DataFrame = indicators match {
    case Nil => dataFrame
    case indicator :: Nil => addSpecificIndicatorColumns(dataFrame, indicator)
    case indicator :: tail => addAllIndicatorColumns(addSpecificIndicatorColumns(dataFrame, indicator), tail)
  }

  private def addSpecificIndicatorColumns(dataFrame: DataFrame, indicator: Indicator)
  : DataFrame = {
    val dictionaryDf = dataFrame.withColumn(keyDictionary, lit(indicator.words))
    val indicatorsColumnName = getIndicatorWordsColumnName(indicator)
    val containedIndicatorsDf = dictionaryDf
      .withColumn(indicatorsColumnName, containedIndicatorWordsUdf(
        dictionaryDf(Keys.text),
        dictionaryDf(Keys.sentence),
        dictionaryDf(Keys.lemma),
        dictionaryDf(keyDictionary)))
      .drop(keyDictionary)
    val flaggedIndicatorDf = containedIndicatorsDf.withColumn(indicator.name,
      hasIndicatorWordUdf(containedIndicatorsDf(indicatorsColumnName)))
    flaggedIndicatorDf
  }

  /**
   * Filters the lemmas for words which are in the indicator dictionary.
   * These selected lemmas have coordinates which are indices in the original text.
   * These indices allow us to extract the original indicator words from the text.
   */
  private val containedIndicatorWordsUdf = udf(
    (textRow: String, sentenceRow: StructArray, lemmaRow: StructArray, indicatorWordsRow: StringArray) => {
      // Filter the lemmas for the words which are in the indicator words dictionary.
      lemmaRow.filter(element => indicatorWordsRow.contains(element.getAs[String](Keys.word)))
        .map(element => {
          // Extract the text coordinates of the original indicator word.
          val wordBegin = element.getAs[Int](Keys.begin)
          val wordEnd = element.getAs[Int](Keys.end)
          // Filter for the one sentence which spans over these coordinates.
          val sentence = sentenceRow.zipWithIndex.filter(element => {
            val sentenceBegin = element._1.getAs[Int](Keys.begin)
            val sentenceEnd = element._1.getAs[Int](Keys.end)
            (sentenceBegin <= wordBegin && wordEnd <= sentenceEnd)
          }).head
          // Extract the coordinates of the one sentence (again).
          val sentenceBegin = sentence._1.getAs[Int](Keys.begin)
          val sentenceEnd = sentence._1.getAs[Int](Keys.end)
          val sentenceIndex = sentence._2
          // Pack all the information up in a new struct for indicator words.
          IndicatorParagraphEntry(
            textRow.substring(wordBegin, wordEnd + 1),
            wordBegin,
            wordEnd,
            sentenceIndex,
            sentenceBegin,
            sentenceEnd)
        })
    })

  /**
   * This checks if a text contained indicators.
   * This is done by looking into the contained indicator words column and checking if there is any word.
   */
  private val hasIndicatorWordUdf = udf((indicatorRow: StructArray) => indicatorRow.nonEmpty)
}
