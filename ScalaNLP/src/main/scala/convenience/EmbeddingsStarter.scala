package convenience


import runners.LocalStarter

object EmbeddingsStarter {
  val experiment = "Embeddings"
  val inputDataPath = "data/cleanedBodyContent"
  val outputDataPath = "data/EmbeddingsOutput"
  val utilsDataPath = "data/utilDictionaries"
  val modelsDataPath = "models"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath, modelsDataPath))
  }
}