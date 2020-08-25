package repositories

import java.io.File
import javax.inject.Singleton
import scala.io.Source
import scala.reflect.runtime.universe.{typeOf, TypeTag}
import play.api.libs.json.Json
import models.{Paragraph, Sentence}

@Singleton
class ImportDataRepository {

  def readParagraphData: List[Paragraph] = {
    readJsonFiles[Paragraph](List[Paragraph]())(
      getListOfFiles("../ScalaNLP/data/indicatorOutput/smoking/paragraph"))
  }

  def readSentenceData: List[Sentence] = {
    readJsonFiles[Sentence](List[Sentence]())(
      getListOfFiles("../ScalaNLP/data/indicatorOutput/smoking/sentence"))
  }

  private def getListOfFiles(directoryPath: String)
  : List[File] = {
    val directory = new File(directoryPath)
    if (directory.exists && directory.isDirectory) {
      directory
        .listFiles.filter(file => file.isFile && file.getName.endsWith(".json")).toList
    } else {
      List[File]()
    }
  }

  @scala.annotation.tailrec
  private def readJsonFiles[T: TypeTag](acc: List[T])(filePaths: List[File])
  : List[T] = filePaths match {
    case Nil => acc
    case x::xs => readJsonFiles[T](readJsonFile[T](x)++acc)(xs)
  }

  private def readJsonFile[T: TypeTag](file: File)
  : List[T] = {
    val source = Source.fromFile(file)
    val jsonLines = source.getLines.toList
    val objects = parseJsonLines[T](List[T]())(jsonLines)
    source.close()
    objects
  }

  @scala.annotation.tailrec
  private def parseJsonLines[T: TypeTag](acc: List[T])(jsonLines: List[String])
  : List[T] = jsonLines match {
    case Nil => acc
    case x::xs => parseJsonLines[T](parseJsonLine[T](x)::acc)(xs)
  }


  private def parseJsonLine[T: TypeTag](jsonLine: String)
  : T = {
    if (typeOf[T] == typeOf[Paragraph]) {
      val json = Json.parse(jsonLine)
      json.as[Paragraph].asInstanceOf[T]
    }
    else if (typeOf[T] == typeOf[Sentence]) {
      val json = Json.parse(jsonLine)
      json.as[Sentence].asInstanceOf[T]
    }
    else throw new IllegalArgumentException("Data type unknown: " + typeOf[T])
  }
}
