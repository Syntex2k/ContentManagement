package models

import play.api.libs.json.{Json, OFormat}

case class Paragraph(paperId: String,
                     paragraphIndex: Int,
                     text: String,
                     indicators: List[ParagraphIndicator])

object Paragraph {
  implicit val format: OFormat[Paragraph] = Json.format[Paragraph]
}