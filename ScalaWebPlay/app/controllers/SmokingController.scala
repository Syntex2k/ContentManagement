package controllers

import javax.inject._
import models.Paragraph
import play.api.libs.json.Json
import play.api.mvc._
import repositories.{ImportDataRepository, SmokingRepository}
import scala.concurrent.ExecutionContext

@Singleton
class SmokingController @Inject()(implicit ec: ExecutionContext,
                                  cc: ControllerComponents,
                                  smokingRepository: SmokingRepository,
                                  importDataRepository: ImportDataRepository) extends AbstractController(cc) {

  def getIndexView: Action[AnyContent] = Action {
    Ok(views.html.index("Corona NLP"))
  }

  def getSmokingView: Action[AnyContent] = Action {
    Ok(views.html.smoking("Smoking Indicator Articles"))
  }

  def getParagraphs: Action[AnyContent] = Action.async {
    smokingRepository.listParagraphs().map(paragraphs => Ok(Json.toJson(paragraphs)))
  }

  def postParagraphsImportCommand: Action[AnyContent] = Action.async {
    val paragraphs = importDataRepository.readParagraphData
    smokingRepository.createParagraphs(paragraphs).map(result => {
      if (result.ok) Ok(result.n.toString)
      else InternalServerError(result.errmsg.getOrElse("Data import failed."))
    })
  }

  def getSentences: Action[AnyContent] = Action.async {
    smokingRepository.listSentences().map(sentences => Ok(Json.toJson(sentences)))
  }

  def getSentence(paperId: String, paragraphIndex: Int, sentenceIndex: Int): Action[AnyContent] = Action.async {
    smokingRepository.findSentencesById(paperId, paragraphIndex, sentenceIndex)
      .map(sentences => Ok(Json.toJson(sentences.head)))
  }

  def getParagraph(paperId: String, paragraphIndex: Int): Action[AnyContent] = Action.async {
    smokingRepository.findParagraphById(paperId, paragraphIndex)
      .map(paragraphs => Ok(Json.toJson(paragraphs.head)))
  }

  def getParagraphSentences(paperId: String, paragraphIndex: Int): Action[AnyContent] = Action.async {
    smokingRepository.findSentencesByParagraph(paperId, paragraphIndex).map(sentences => Ok(Json.toJson(sentences)))
  }

  def postSentencesImportCommand: Action[AnyContent] = Action.async {
    val sentences = importDataRepository.readSentenceData
    sentences.foreach(s => println(s.toString))
    smokingRepository.createSentences(sentences).map(result => {
      if (result.ok) Ok(result.n.toString)
      else InternalServerError(result.errmsg.getOrElse("Data import failed."))
    })
  }
}
