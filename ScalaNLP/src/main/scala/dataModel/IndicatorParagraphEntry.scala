package dataModel

case class IndicatorParagraphEntry(word: String,
                                   begin: Int,
                                   end: Int,
                                   sentenceIndex: Int,
                                   sentenceBegin: Int,
                                   sentenceEnd: Int)