package experiments

import annotators._
import dataIO.{InputDataLoader, JsonFileUtils}
import dataModel.{DataPaths, Keys}
import logging.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipelines._

class IndicatorAnalysis(spark: SparkSession, dataPaths: DataPaths) {
  private val debugPreProcessing = true
  private val debugCleanup = true
  private val debugIndicatorAnalysis = true
  private val debugFoldedSentences = true
  private val debugUnfoldedSentences = true
  private val debugVectorization = true
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
    val smokingParagraphOutputColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.text, smokingWordsColumnName)
    saveIndicatorParagraphs(smokingParagraphsDf, smokingIndicator, smokingParagraphOutputColumns)
    // Now that we have reduced the number of paragraphs, we can have a closer look at the sentences.
    val smokingSentencesDf = analyseIndicatorSentences(smokingParagraphsDf, smokingIndicator)
    saveIndicatorSentences(smokingSentencesDf, smokingIndicator)
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

  private def analyseIndicatorSentences(indicatorDf: DataFrame, indicator: Indicator)
  : DataFrame = {
    // Extract the just sentences from the paragraphs which hold an indicator word.
    val sentencesDf = new IndicatorSentenceExtractor(spark).extract(indicatorDf, indicator)
    // Plot the data frame to the console for schema and data inspection.
    if (debugFoldedSentences) Logger.printDataFrameForDebug(
      s"Indicator[${indicator.name}] Sentences Folded:", sentencesDf, 5, 180)
    // As the output of the sentence extraction is folded in one column with a complex struct we need to unfold.
    val unfoldedColumns = Seq(Keys.paperId, Keys.paragraphIndex, Keys.sentenceIndex,
      Keys.text, Keys.begin, Keys.end, Keys.indicators, Keys.similar, Keys.sentimentScore)
    val unfoldedSentencesDf = SentenceUtils.unfoldSentencesDataFrame(sentencesDf, Keys.sentence, unfoldedColumns)
    if (debugUnfoldedSentences) Logger.printDataFrameForDebug(
      s"Indicator[${indicator.name}] Sentences Unfolded:", unfoldedSentencesDf, 5, 20)

    // Run the port of speech analysis pipeline on the sentences (with indicator words.).
    val posOutputDf = new PosPipeline(spark, dataPaths.models).run(unfoldedSentencesDf)
    val cleanPosDf = PosFinisher.cleanup(posOutputDf)
    analyseSentenceSimilarities(cleanPosDf.drop(Keys.similar))
  }

  private def analyseSentenceSimilarities(sentencesDf: DataFrame)
  : DataFrame = {
    // Setup the hyper parameters for our vectorization training processes.
    val dataLoader = new InputDataLoader(dataPaths)
    val stopWords = dataLoader.loadStopWords() ++ dataLoader.loadCoronaStopWords()
    val vocabularySize = 1000
    val vectorizerParams: VectorizerParameters = VectorizerParameters(vocabularySize, stopWords)
    // Build the vectorization pipeline pipeline and run it on the extracted sentences.
    val (vectorizedDf, vocabulary) = new CountVectorPipeline(spark, dataPaths.models, vectorizerParams).run(sentencesDf)
    // Setup the hyper parameters for our min hashing training processes.
    val minHashParams = MinHashParameters(5, 0.9, vocabularySize, vocabulary)
    // Now calculate the hash values for the vectors. Note that the data frame passed in the constructor.
    // We will also find one other sentence which is similar based on the hash values.
    val hashedDf = new MinHashPipeline(spark, minHashParams, vectorizedDf).run(vectorizedDf)
    // We group aggregate each similar entry into one list of entries.
    val resultDf = hashedDf.groupBy(hashedDf(Keys.paperId), hashedDf(Keys.paragraphIndex), hashedDf(Keys.sentenceIndex))
      .agg(first(col(Keys.text)).as(Keys.text),
        first(col(Keys.begin)).as(Keys.begin),
        first(col(Keys.end)).as(Keys.end),
        first(col(Keys.indicators)).as(Keys.indicators),
        first(col(Keys.sentimentScore)).as(Keys.sentimentScore),
        first(col(Keys.pos)).as(Keys.pos),
        collect_list(col(Keys.similar)).as(Keys.similar))
    if (debugVectorization) Logger.printDataFrameForDebug(
      s"Aggregates Sentences:", resultDf, 5, 20)
    resultDf
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

  private def saveIndicatorSentences(indicatorDf: DataFrame, indicator: Indicator)
  : Unit = {
    // Plot the data frame to the console for schema and data inspection.
    if (debugOutput) Logger.printDataFrameForDebug(
      s"Indicator[${indicator.name}] Sentences Output:", indicatorDf, 5, 30)
    // Save the data frame with indicator sentences as json line files.
    JsonFileUtils.saveAsJson(indicatorDf, dataPaths.output + "/" + indicator.name + "/" + Keys.sentence)
  }
}
