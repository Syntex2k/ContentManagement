package dataIO

import java.io.File

import dataModel.InputKeys
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class JsonFileUtilsTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val testInputFolder = "data/testData/dataIO"
  val testOutputFolder = "data/testDataOutput"


  override protected def beforeAll() {
    val spark = SparkSession.builder.
      appName("SparkSessionExample").
      master("local[24]").
      getOrCreate
  }

  override protected def beforeEach(): Unit = {
    cleanUpOutputDirectory()
  }

  test("parse json correctly") {
    val dataFrameRow = JsonFileUtils.readJsonToDataFrame(testInputFolder).first();

    val map = dataFrameRow.getValuesMap(Seq("paper_id", "body_text"));
    assert(map.getOrElse("paper_id", 0) === "paperId1")
    assert(map.get("body_text").toString.contains("The role of ubiquitination and the proteins that regulate it are the focus"))
  }

  test("parse more json and put them into one data frame") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)

    val numberOfRows = dataFrame.count()
    assert(numberOfRows === 3)
  }

  test("should correctly save the data frame as json") {
    val dataFrame = JsonFileUtils.readJsonToDataFrame(testInputFolder)

    JsonFileUtils.saveAsJson(dataFrame, testOutputFolder);

    val dataFrameNew = JsonFileUtils.readJsonToDataFrame(testOutputFolder, false);
    dataFrame.show();
    val numberOfRows = dataFrameNew.count()
    assert(numberOfRows === 3);
  }

  test("should list all files in directory correctly") {
    val result = JsonFileUtils.getListOfFilesInFolder(testInputFolder)

    val expected = List("rawMinimal.json", "rawMinimal2.json", "rawMinimal3.json")
    assert(result === expected)
  }

  test("should correctly read utility dictionaries") {
    val stopWords = JsonFileUtils.readDictionaryWordsJsonToArray("data/utilDictionaries/EnglishStopWords.json", InputKeys.stopWords)

    val expected = Array("able", "about", "above", "abroad", "according", "accordingly", "across", "actually", "adj") //ausschnitt

    expected.foreach(word => {
      assert(stopWords.contains(word))
    })
  }


  test("should read the file and collect all columns provided") {
    val result = JsonFileUtils.readInputJsonToDataFrame(testInputFolder, Seq("paper_id")).first()

    val columns = result.schema.fields.map(x => x.name)
    val expectedColumns = List("paper_id")
    assert(columns === expectedColumns)
  }

  test("should read the file and collect all columns provided 2") {
    val result = JsonFileUtils.readInputJsonToDataFrame(testInputFolder, Seq("paper_id", "body_text")).first()

    val columns = result.schema.fields.map(x => x.name)
    val expectedColumns = List("paper_id", "body_text")
    assert(columns === expectedColumns)
  }

  def cleanUpOutputDirectory(): Unit = {
    val dir = new File(testOutputFolder);
    if (dir.isDirectory) {
      dir.listFiles().foreach(file => {
        file.delete()
      })
    }
  }
}
