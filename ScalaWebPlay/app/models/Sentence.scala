package models

import play.api.libs.json.{Json, OFormat}

case class Sentence(paperId: String,
                    paragraphIndex: Int,
                    sentenceIndex: Int,
                    text: String,
                    begin: Int,
                    end: Int,
                    similar: List[SimilarSentence],
                    indicators: List[SentenceIndicator],
                    pos: List[PosAnnotation],
                    sentimentScore: Double)

object Sentence {
  implicit val format: OFormat[Sentence] = Json.format[Sentence]
}