package dataIO

import dataModel.{DataPaths, InputKeys, Keys}
import logging.Logger
import org.apache.spark.sql.DataFrame

class InputDataLoader(dataPaths: DataPaths) {
  private val debugStopWords = true
  private val debugCoronaStopWords = true
  private val debugSmokingWords = true

  /**
   * Loads in the cleaned text data we want to process.
   *
   * @return data frame with paragraphs from all papers as main rows
   */
  def loadInputDataFrame()
  : DataFrame = {
    val inputColumns = Seq(InputKeys.paperId, InputKeys.paragraphIndex, Keys.text)
    JsonFileUtils.readInputJsonToDataFrame(dataPaths.input, inputColumns)
      .withColumnRenamed(InputKeys.paperId, Keys.paperId)
      .withColumnRenamed(InputKeys.paragraphIndex, Keys.paragraphIndex)
  }

  /**
   * Loads in a list of stop word which do not carry much meaning.
   *
   * @return string array listing english stop words
   */
  def loadStopWords()
  : Array[String] = {
    val stopWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.stopWords)
    if (debugStopWords) Logger.printWordListForDebug("Stop Words:", stopWords)
    stopWords
  }

  /**
   * Loads in a list of stop word which appear to often in corona related texts.
   *
   * @return string array listing custom corona stop words
   */
  def loadCoronaStopWords()
  : Array[String] = {
    val coronaStopWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.coronaStopWords)
    if (debugCoronaStopWords) Logger.printWordListForDebug("Corona stop Words:", coronaStopWords)
    coronaStopWords
  }

  /**
   * Loads in a list of smoking related words.
   *
   * @return string array listing the words linked to smoking
   */
  def loadSmokingWords()
  : Array[String] = {
    val smokingWords = JsonFileUtils.readDictionaryWordsJsonToArray(dataPaths.utils, InputKeys.smokingWords)
    if (debugSmokingWords) Logger.printWordListForDebug("Smoking Words:", smokingWords)
    smokingWords
  }
}
