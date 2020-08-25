package models

import play.api.libs.json.{Json, OFormat}

case class SimilarSentence(paperId: String, paragraphIndex: Int, sentenceIndex: Int, distance: Double)

object SimilarSentence {
  implicit val format: OFormat[SimilarSentence] = Json.format[SimilarSentence]
}