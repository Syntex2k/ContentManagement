package experiments

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import annotators.{Indicator, IndicatorAnnotator, LemmaFinisher, SentenceFinisher}
import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.{DataPaths, Keys}
import logging.Logger
import pipelines.{PreprocessingPipeline}

class SmokingIndicator(spark: SparkSession, dataPaths: DataPaths) {
  private val debugPreProcessing = true
  private val debugCleanup = true
  private val debugIndicatorAnalysis = true

  private val debugOutput = true

  def start()
  : Unit = {
    // Load in initial input data and some util dictionaries (word lists).
    val dataLoader = new InputDataLoader(dataPaths)
    val inputDf = dataLoader.loadInputDataFrame()
    val stopWords = dataLoader.loadStopWords()
    val smokingWords = dataLoader.loadSmokingWords()
    // Run the basic data pre-processing steps. (lemma, tokens, normal etc.)
    val preProcessedDf = preProcessData(inputDf, stopWords);
    // Now analyse the paragraphs for indicators pointing at links with smoking habits.
    val smokingIndicator = Indicator(Keys.smoking, smokingWords)
    val smokingParagraphsDf = analyseIndicatorParagraphs(preProcessedDf, smokingIndicator)
    // Store the final result for the smoking indicator paragraphs.
    val smokingWordsColumnName = IndicatorAnnotator.getIndicatorWordsColumnName(smokingIndicator)
    val smokingParagraphOutputColumns = Seq(Keys.paperId, smokingWordsColumnName)
    saveIndicatorParagraphs(smokingParagraphsDf, smokingIndicator, smokingParagraphOutputColumns)
    // Now that we have reduced the number of paragraphs, we can have a closer look at the sentences.
  }

  private def preProcessData(inputDf: DataFrame, stopWords: Array[String])
  : DataFrame = {
    // Build the pre-processing ml pipeline and run it.
    val preProcessingDf = new PreprocessingPipeline(spark, stopWords, dataPaths.models).run(inputDf)
    // Plot the data frame to the console for schema and data inspection.
    if (debugPreProcessing) {
      Logger.printDataFrameForDebug("PreProcessing:", preProcessingDf, 5, 20)
    }
    // Now clean up the pipeline result by removing unnecessary information.
    val cleanupColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.text, Keys.sentence, Keys.lemma)
    // Select only the columns we are interested in from the data frame.
    val cleanupDf = preProcessingDf.select(cleanupColumns.map(c => col(c)): _*)
    // Clean up the entries in the sentence and lemma column and build leaner data structures
    val cleanDf = LemmaFinisher.cleanup(SentenceFinisher.cleanup(cleanupDf))
    // Plot the data frame to the console for schema and data inspection.
    if (debugCleanup) Logger.printDataFrameForDebug(
      "Cleanup:", cleanDf, 5, 30)
    cleanDf
  }

  private def analyseIndicatorParagraphs(preProcessedDf: DataFrame, indicator: Indicator)
  : DataFrame = {
    // Add columns identifying texts with indicator words.
    val indicatorDf = IndicatorAnnotator.annotate(preProcessedDf, List(indicator))
    // The column names with the indicator data depend on the risk factor we researched.
    // The indicator annotator created the columns. Therefore it has methods to acquire the names.
    val indicatorFagsColumnName = IndicatorAnnotator.getIndicatorFlagColumnName(indicator)
    val indicatorWordsColumnName = IndicatorAnnotator.getIndicatorWordsColumnName(indicator)
    val indicatorColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.text, Keys.sentence,
      indicatorFagsColumnName, indicatorWordsColumnName)
    // Select only the columns we are interested in from the data frame.
    val indicatorOutputDf = indicatorDf.select(indicatorColumns.map(c => col(c)): _*)
      // Now filter for only the paragraphs (rows) which are linked to the indicator.
      .filter(indicatorDf(indicator.name)).cache()
    // Plot the data frame to the console for schema and data inspection.
    if (debugIndicatorAnalysis) Logger.printDataFrameForDebug(
      "Indicator Analysis:", indicatorOutputDf, 5, 20)
    indicatorOutputDf
  }


  private def saveIndicatorParagraphs(indicatorDf: DataFrame, indicator: Indicator, outputColumns: Seq[String])
  : Unit = {
    // Select only the columns we are interested in from the data frame.
    val indicatorWordsColumnName = IndicatorAnnotator.getIndicatorWordsColumnName(indicator)
    val outputDf = indicatorDf.select(outputColumns.map(c => col(c)): _*)
      // Rename some columns to fit our database schema.
      .withColumnRenamed(indicatorWordsColumnName, Keys.indicators)
    // Plot the data frame to the console for schema and data inspection.
    if (debugOutput) Logger.printDataFrameForDebug(
      s"Indicator[${indicator.name}] Paragraphs Output:", outputDf, 5, 30)
    // Save the data frame with indicator paragraphs as json line files.
    JsonFileUtils.saveAsJson(outputDf, dataPaths.output + "/" + indicator.name + "/" + Keys.paragraph)
  }
}
