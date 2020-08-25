package dataIO

import java.io.PrintWriter

import ujson.Value

import scala.io.Source

object JsonCleaner {
  val keyPaperId = "paper_id"
  val keyTextAbstract = "abstract"
  val keyTextBody = "body_text"
  val keyParagraphIndex = "paragraph_index"
  val keyCiteSpans = "cite_spans"
  val keyRefSpans = "ref_spans"
  val keySection = "section"

  /**
   * Cleans the data so that only the main text section remains with some metadata.
   *
   * @param fileName        name of the file to clean
   * @param inputDirectory  folder where the input files reside
   * @param outputDirectory name of the folder where to put the output
   * @param fileSection     to extract (e.g. abstract or main text body)
   */
  def extractJsonTextData(fileName: String,
                          inputDirectory: String,
                          outputDirectory: String,
                          fileSection: String)
  : Unit = {
    val data = readJsonFile(inputDirectory, fileName)
    val textSectionData = data(fileSection)
    if (textSectionData.arr.nonEmpty) {
      textSectionData.arr.indices.foreach(paragraphIndex => {
        cleanParagraphMetaData(textSectionData(paragraphIndex), paragraphIndex, data(keyPaperId))
      })
      writeJsonFile(textSectionData, outputDirectory, fileName)
    }
  }

  /**
   * Helper function to clean up the paragraph data and ad some meta information.
   *
   * @param paragraph      to clean
   * @param paragraphIndex used to identify the paragraph later on
   * @param paperId        used to sort the paragraph to a paper
   */
  private def cleanParagraphMetaData(paragraph: Value, paragraphIndex: Int, paperId: Value)
  : Unit = {
    // Remove links to other texts.
    paragraph.obj.remove(keyCiteSpans)
    paragraph.obj.remove(keyRefSpans)
    paragraph.obj.remove(keySection)
    paragraph(keyParagraphIndex) = paragraphIndex
    paragraph(keyPaperId) = paperId
  }

  /**
   * Uses uJson to read in json file and return value for a text section
   *
   * @param inputDirectory folder where the json file is located
   * @param fileName       name of the json to load
   * @return ata in form of uJson values
   */
  private def readJsonFile(inputDirectory: String, fileName: String)
  : Value = {
    val inputPath = inputDirectory + "/" + fileName
    val source = Source.fromFile(inputPath)
    val data = ujson.read(source.getLines().mkString)
    source.close()
    data
  }

  /**
   * Used uJson to save text data to json files.
   *
   * @param textData        to persist
   * @param outputDirectory where to put the json file
   * @param fileName        the json file should be given
   */
  private def writeJsonFile(textData: Value, outputDirectory: String, fileName: String)
  : Unit = {
    val outputJson = ujson.write(textData)
    new PrintWriter(outputDirectory + "/" + fileName) {
      write(outputJson)
      close()
    }
  }
}
