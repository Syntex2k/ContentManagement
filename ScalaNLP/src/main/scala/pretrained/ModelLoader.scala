package pretrained

object ModelLoader {
  private val lemmatizerModelName = "lemma_antbnc_en_2.0.2_2.4_1556480454569"
  private val posModelName = "pos_anc_en_2.0.2_2.4_1556659930154"
  private val bertBaseCased = "bert_base_cased_en_2"
  private val sentimentModelName = "analyze_sentiment_en_2.4.0_2.4_1580483464667"


  def getLemmatizerModelPath(modelsDirectory: String): String = joinModelPath(modelsDirectory, lemmatizerModelName)
  def getBertModelPath(modelsDirectory: String): String = joinModelPath(modelsDirectory,bertBaseCased)
  def getPosPerceptronModelPath(modelsDirectory: String): String = joinModelPath(modelsDirectory, posModelName)
  def getSentimentAnalyzerModelPath(modelsDirectory: String): String = joinModelPath(modelsDirectory, sentimentModelName)

  private def joinModelPath(modelsDirectory: String, modelName: String)
  : String = modelsDirectory + "/" + modelName.toString
}
