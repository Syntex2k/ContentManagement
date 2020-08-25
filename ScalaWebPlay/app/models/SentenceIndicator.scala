package models

import play.api.libs.json.{Json, OFormat}

case class SentenceIndicator(word: String, begin: Int, end: Int)

object SentenceIndicator {
  implicit val format: OFormat[SentenceIndicator] = Json.format[SentenceIndicator]
}