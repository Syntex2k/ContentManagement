package convenience

import runners.LocalStarter

object SentimentAnalysisStarter {

  val experiment = "SentimentAnalysis"
  val inputDataPath = "data/indicatorOutput/smoking/sentence"
  val outputDataPath = "data/sentimentOutput"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}
