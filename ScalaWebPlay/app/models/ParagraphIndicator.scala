package models

import play.api.libs.json.{Json, OFormat}

case class ParagraphIndicator(word: String,
                              begin: Int,
                              end: Int,
                              sentenceIndex: Int,
                              sentenceBegin: Int,
                              sentenceEnd: Int)

object ParagraphIndicator {
  implicit val format: OFormat[ParagraphIndicator] = Json.format[ParagraphIndicator]
}