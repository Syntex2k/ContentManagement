package convenience

import runners.LocalStarter

object TopicExtractionStarter {
  val experiment = "TopicExtraction"
  val inputDataPath = "data/cleanedBodyContent"
  val outputDataPath = "data/topicOutput/rawSpark"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}
