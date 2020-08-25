package logging

import org.apache.spark.sql.DataFrame

object Logger {

  def printWordListForDebug(headline: String, words: Array[String])
  : Unit = {
    println("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    println(headline)
    words.foreach(word => print(s"$word | "))
    println()
  }

  def printDataFrameForDebug(headline: String, dataFrame: DataFrame, numRows: Int, truncate: Int)
  : Unit = {
    println("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
    println(headline)
    dataFrame.show(5, truncate = truncate)
    dataFrame.printSchema()
  }
}
