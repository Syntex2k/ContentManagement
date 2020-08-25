package dataIO

import java.io.File

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object JsonFileUtils {
  private val spark: SparkSession = SparkSession.builder().getOrCreate();
  spark.sparkContext.setLogLevel("WARN")
  val jsonFileFormatSource = "org.apache.spark.sql.execution.datasources.json.JsonFileFormat"
  val jsonSuffix = ".json"
  val crcSuffix = ".crc"

  def readInputJsonToDataFrame(jsonDataFolder: String, selectedKeys: Seq[String])
  : DataFrame = {
    val jsonInputDf = JsonFileUtils.readJsonToDataFrame(jsonDataFolder)
    jsonInputDf.select(selectedKeys.map(key => col(key)): _*)
  }

  /**
   * Reads all the jsons from a file or a folder into a spark data frame.
   * Spark supports reading jsons in two different ways.
   * The standard procedure is that a json file consists of only one line.
   * Each line is interpreted as a standalone json.
   * Because our data is not in a single line representation, we need to set the multiline flag.
   * It escapes the behaviour and interprets a whole file as a json.
   *
   * @param inputFolder where to find the json data
   * @param multiline   read json per file or per line
   * @param verbose     print dataset to console
   * @return DataFrame object with data to use in spark
   */
  def readJsonToDataFrame(inputFolder: String, multiline: Boolean = true, verbose: Boolean = false)
  : DataFrame = {
    val dataFrame = spark.read.format(jsonFileFormatSource).option("multiLine", multiline).load(inputFolder)
    if (verbose) dataFrame.show(truncate = true)
    dataFrame
  }

  /**
   * Saves a spark data frame into one json file.
   *
   * @param dataFrame    to store as json
   * @param outputFolder where to put the json data
   */
  def saveAsJson(dataFrame: DataFrame, outputFolder: String)
  : Unit = {
    dataFrame.write.format(jsonFileFormatSource).mode("append").save(outputFolder)
  }

  /**
   * Gets the name of all files inside a given directory.
   *
   * @param directory path to the directory
   * @param suffix    the file ends with
   * @return list of file names
   */
  def getListOfFilesInFolder(directory: String, suffix: String = jsonSuffix)
  : List[String] = {
    val file = new File(directory)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(suffix))
      .map(_.getName)
      .toList
  }

  /**
   * Reads in a utility dictionary from json. This is just a list of words.
   *
   * @param jsonDataFolder where to search for the utility dictionary
   * @param dictionaryKey  key identifying the specific dictionary to use
   * @return array of word in the dictionary
   */
  def readDictionaryWordsJsonToArray(jsonDataFolder: String, dictionaryKey: String)
  : Array[String] = {
    val dictionaryDf = JsonFileUtils.readInputJsonToDataFrame(jsonDataFolder, Seq(dictionaryKey))
    dictionaryDf.select(dictionaryKey).na.drop().collect().toList.foldLeft(List[String]())(
      (list, row) => list ++ row.getAs[mutable.WrappedArray[String]](dictionaryKey).toList).toArray
  }
}
