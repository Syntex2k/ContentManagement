package models

import play.api.libs.json.{Json, OFormat}

case class PosAnnotation(word: String, tag: String, begin: Int, end: Int)

object PosAnnotation {
  implicit val format: OFormat[PosAnnotation] = Json.format[PosAnnotation]
}