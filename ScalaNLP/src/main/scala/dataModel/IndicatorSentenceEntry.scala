package dataModel

case class IndicatorSentenceEntry(paperId: String,
                                  paragraphIndex: Long,
                                  sentenceIndex: Int,
                                  text: String,
                                  begin: Int,
                                  end: Int,
                                  indicators: List[IndicatorWordEntry],
                                  similar: List[SimilarSentenceEntry],
                                  sentimentScore: Double)
