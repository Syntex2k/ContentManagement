package experiments

import dataIO.{JsonCleaner, JsonFileUtils}
import dataModel.{DataPaths, InputKeys, Keys}
import org.apache.spark.sql.SparkSession


class LocalBodyCleaner(spark: SparkSession, dataPaths: DataPaths) {


  def start()
  : Unit = {

    println("Input: " + dataPaths.input)
    println("Output: " + dataPaths.output)
    val listOfFilesInDirectory = JsonFileUtils.getListOfFilesInFolder(dataPaths.input)
    listOfFilesInDirectory.foreach(file => {
      JsonCleaner.extractJsonTextData(file, dataPaths.input, dataPaths.output, JsonCleaner.keyTextBody);
    })
  }
}


