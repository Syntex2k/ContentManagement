package convenience

import runners.LocalStarter

object IndicatorAnalysisStarter {
  val experiment = "IndicatorAnalysis"
  val inputDataPath = "data/cleanedBodyContent"
  val outputDataPath = "data/indicatorOutput"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}
