package convenience

import runners.LocalStarter

object SmokingIndicatorStarter {
  val experiment = "SmokingIndicator"
  val inputDataPath = "data/LocalBodyCleanerOutput"
  val outputDataPath = "data/SmokingIndicatorOutput"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}