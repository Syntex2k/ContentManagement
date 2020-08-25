package dataIO

import org.apache.spark.sql.SparkSession

object StartJobBodyExtraction {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("JsonCleaning")
    .master("local[*]")
    .config("spark.driver.memory", "12G")
    .getOrCreate()

  /**
   * Starts the text body extraction and cleaning pipeline.
   */
  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)
    val outputDirectory = args(1)

    println("Input: " + inputDirectory)
    println("Output: " + outputDirectory)

    val listOfFilesInDirectory = JsonFileUtils.getListOfFilesInFolder(inputDirectory)
    listOfFilesInDirectory.foreach(file => {
      JsonCleaner.extractJsonTextData(file, inputDirectory, outputDirectory, JsonCleaner.keyTextBody);
    })
  }
}
