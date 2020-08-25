package convenience

import runners.LocalStarter

object LocalBodyCleanerStarter {
  val experiment = "LocalBodyCleaner"
  val inputDataPath = "/Users/Tobias/Desktop/Content_Management/kaggle_data/document_parses/pdf_json"
  val outputDataPath = "data/LocalBodyCleanerOutput"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}