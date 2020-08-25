package pretrained

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class ModelLoaderTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val modelDirectory = "models"

  test("should join model path for Pos Model correctly") {
    val result = ModelLoader.getPosPerceptronModelPath(modelDirectory)
    val expected = "models/pos_anc_en_2.0.2_2.4_1556659930154"

    assert(result === expected)
  }

  test("should join model path for Sentiment Analyzer Model correctly") {
    val result = ModelLoader.getSentimentAnalyzerModelPath(modelDirectory)
    val expected = "models/analyze_sentiment_en_2.4.0_2.4_1580483464667"

    assert(result === expected)
  }

  test("should join model path for Lemmatizer Model correctly") {
    val result = ModelLoader.getLemmatizerModelPath(modelDirectory)
    val expected = "models/lemma_antbnc_en_2.0.2_2.4_1556480454569"

    assert(result === expected)
  }
}
