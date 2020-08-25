package runners

import dataModel.DataPaths
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * This class should be handed over to the spark session via spark-submit to start experiments on the cluster.
 * This serves to setup specific file paths which are specific to the hadoop file system.
 */
object HdfsStarter {
  private val spark: SparkSession = SparkSession.builder().appName("Pipeline").getOrCreate()
  // Base path for the hadoop file system on the HTW server.
  private val hdfsBasePath = "hdfs://hadoop05.f4.htw-berlin.de:8020/user/"
  // Hadoop file system user where we have placed our source data from Kaggle.
  private val hdfsSourceUser = "s0563263"

  /**
   * example call:
   * spark-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.5.2 \
   * --class experiments.HdfsStarter \
   * --master yarn target/scala-2.11/scalanlp_2.11-0.1.jar \
   * IndicatorAnalysis \
   * data/cleanedBodyContent \
   * data/indicatorOutput \
   * data/utilDictionaries \
   * models \
   * s0563413
   *
   * @param args experiment-name input-data-folder output-data-folder util-data-folder model-data-folder target-user
   */
  def main(args: Array[String])
  : Unit = {
    val experimentName = args(0)
    val inputDataFolder = args(1)
    val outputDataFolder = args(2)
    val utilDataFolder = args(3)
    val modelsFolder = args(4)
    val hdfsTargetUser = args(5)

    val hdfsInputDataDir = new Path(hdfsBasePath + hdfsSourceUser + "/" + inputDataFolder + "/").toUri.toString
    val hdfsOutputDataDir = new Path(hdfsBasePath + hdfsTargetUser + "/" + outputDataFolder).toUri.toString
    val hdfsUtilDataDir = new Path(hdfsBasePath + hdfsSourceUser + "/" + utilDataFolder).toUri.toString
    val hdfsModelDataDir = new Path(hdfsBasePath + hdfsSourceUser + "/" + modelsFolder).toUri.toString
    val dataPaths = DataPaths(hdfsInputDataDir, hdfsOutputDataDir, hdfsUtilDataDir, hdfsModelDataDir)
    ExperimentSelector.startExperiment(spark, experimentName, dataPaths)
  }
}
