package repositories

import javax.inject.{Inject, Singleton}
import play.api.libs.json.{JsObject, Json}
import play.modules.reactivemongo.ReactiveMongoApi
import reactivemongo.api.{Cursor, ReadPreference}
import reactivemongo.api.commands.MultiBulkWriteResult
import reactivemongo.play.json._
import reactivemongo.play.json.collection.JSONCollection
import scala.concurrent.{ExecutionContext, Future}
import models.{Paragraph, Sentence}

/**
 * Repository for accessing the Mongo database for smoking text analysis data.
 * @param ec execution context
 * @param reactiveMongoApi
 */
@Singleton
class SmokingRepository @Inject()(implicit ec: ExecutionContext, reactiveMongoApi: ReactiveMongoApi) {

  private def paragraphsCollectionFuture: Future[JSONCollection] =
    reactiveMongoApi.database.map(db => db.collection("paragraphs"))

  def listParagraphs(limit: Int = 100): Future[Seq[Paragraph]] = paragraphsCollectionFuture
    .flatMap(paragraphsCollection => paragraphsCollection
      .find(Json.obj(), projection = Option.empty[JsObject])
      .cursor[Paragraph](ReadPreference.primary)
      .collect[Seq](limit, Cursor.FailOnError[Seq[Paragraph]]()))

  def createParagraphs(paragraphs: List[Paragraph]): Future[MultiBulkWriteResult] = {
    paragraphsCollectionFuture.flatMap(paragraphsCollection => paragraphsCollection
      .insert(false).many(paragraphs))
  }

  private def sentencesCollectionFuture: Future[JSONCollection] =
    reactiveMongoApi.database.map(db => db.collection("sentences"))

  def listSentences(limit: Int = 50): Future[Seq[Sentence]] = sentencesCollectionFuture
    .flatMap(sentencesCollection => sentencesCollection
      .find(Json.obj(), projection = Option.empty[JsObject])
      .cursor[Sentence](ReadPreference.primary)
      .collect[Seq](limit, Cursor.FailOnError[Seq[Sentence]]()))

  def findSentencesByParagraph(paperId: String, paragraphIndex: Int): Future[Seq[Sentence]] = sentencesCollectionFuture
    .flatMap(sentencesCollection => {
      val query = Json.obj("paperId" -> paperId, "paragraphIndex" -> paragraphIndex)
      sentencesCollection
        .find(query, projection = Option.empty[JsObject])
        .cursor[Sentence](ReadPreference.primary)
        .collect[Seq](10, Cursor.FailOnError[Seq[Sentence]]())
    })

  def findSentencesById(paperId: String, paragraphIndex: Int, sentenceIndex: Int): Future[Seq[Sentence]] = sentencesCollectionFuture
  .flatMap(sentencesCollection => {
    val query = Json.obj("paperId" -> paperId, "paragraphIndex" -> paragraphIndex, "sentenceIndex" -> sentenceIndex)
    sentencesCollection
      .find(query, projection = Option.empty[JsObject])
      .cursor[Sentence](ReadPreference.primary)
      .collect[Seq](1, Cursor.FailOnError[Seq[Sentence]]())
  })

  def findParagraphById(paperId: String, paragraphIndex: Int): Future[Seq[Paragraph]] = paragraphsCollectionFuture
    .flatMap(paragraphCollection => {
      val query = Json.obj("paperId" -> paperId, "paragraphIndex" -> paragraphIndex)
      paragraphCollection
        .find(query, projection = Option.empty[JsObject])
        .cursor[Paragraph](ReadPreference.primary)
        .collect[Seq](1, Cursor.FailOnError[Seq[Paragraph]]())
    })

  def createSentences(sentences: List[Sentence]): Future[MultiBulkWriteResult] = {
    sentencesCollectionFuture.flatMap(sentencesCollection => sentencesCollection
      .insert(false).many(sentences))
  }
}
