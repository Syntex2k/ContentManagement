package dataIO

import org.scalatest.funsuite.AnyFunSuite

class JsonCleanerTest extends AnyFunSuite {

  test("should have the correct paper_id value") {
    assert(JsonCleaner.keyPaperId === "paper_id")
  }

  test("should have the correct abstract value") {
    assert(JsonCleaner.keyTextAbstract === "abstract")
  }

  test("should have the correct body_text value") {
    assert(JsonCleaner.keyTextBody === "body_text")
  }

  test("should have the correct paragraph_index value") {
    assert(JsonCleaner.keyParagraphIndex === "paragraph_index")
  }

  test("should have the correct cite_spans value") {
    assert(JsonCleaner.keyCiteSpans === "cite_spans")
  }

  test("should have the correct ref_spans value") {
    assert(JsonCleaner.keyRefSpans === "ref_spans")
  }

  test("should have the correct section value") {
    assert(JsonCleaner.keySection === "section")
  }
}
