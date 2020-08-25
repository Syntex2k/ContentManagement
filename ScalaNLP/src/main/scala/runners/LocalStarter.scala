package runners

import dataModel.DataPaths
import org.apache.spark.sql.SparkSession

/**
 * This class cn be submitted to spark when testing experiments on a local machine.
 * It will setup file paths which refer to the IntelliJ project structure with some sample data.
 */
object LocalStarter {
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("IndicatorAnalysis")
    .master("local[*]")
    .config("spark.driver.memory", "12G")
    .getOrCreate()

  /**
   * example call:
   * spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.5.2 \
   * --class experiments.LocalStarter \
   * --master "local[*]" target/scala-2.11/scalanlp_2.11-0.1.jar \
   * IndicatorAnalysis \
   * data/cleanedBodyContent \
   * data/indicatorOutput \
   * data/utilDictionaries \
   * models
   *
   * @param args experiment-name input-data-folder output-data-folder util-data-folder model-data-folder
   */
  def main(args: Array[String])
  : Unit = {
    val experimentName = args(0)
    val inputDataDir = args(1)
    val outputDataDir = args(2)
    val utilDataDir = args(3)
    val modelsDir = args(4)

    val dataPaths = DataPaths(inputDataDir, outputDataDir, utilDataDir, modelsDir)
    ExperimentSelector.startExperiment(spark, experimentName, dataPaths)
  }
}
