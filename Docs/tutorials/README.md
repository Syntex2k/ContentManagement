# Spark NLP Tutorials
Helpful information about the framework. Lets collect good tutorials helpful links here. We can also add snippets which might find their way into the research paper later on. 

***
## Articles

https://medium.com/spark-nlp

- Overview

  - https://towardsdatascience.com/introduction-to-spark-nlp-foundations-and-basic-components-part-i-c83b7629ed59
- Installation

  - https://medium.com/spark-nlp/introduction-to-spark-nlp-installation-and-getting-started-part-ii-d009f7a177f3
- Document Assembler
  - https://medium.com/spark-nlp/spark-nlp-101-document-assembler-500018f5f6b5

## Building Blocks

### Document Assembler
As discussed before, each annotator in Spark NLP accepts certain types of columns and outputs new columns in another type (we call this AnnotatorType). In Spark NLP, we have the following types:

**Document, token, chunk, pos, word_embeddings, date, entity, sentiment, named_entity, dependency, labeled_dependency.**

To get through the process in Spark NLP, we need to get raw data transformed into Document type at first. DocumentAssembler() is a special transformer that does this for us; it creates the first annotation of type Document which may be used by annotators down the road. DocumentAssembler() comes from sparknlp.base class and has the following settable parameters. See the full list here and the source code here.

- setInputCol() -> the name of the column that will be converted. We can specify only one column here. It can read either a String column or an Array[String]

- setOutputCol() -> optional : the name of the column in Document type that is generated. We can specify only one column here. Default is ‘document’

- setIdCol() -> optional: String type column with id information

- setMetadataCol() -> optional: Map type column with metadata information

- setCleanupMode() -> optional: Cleaning up options, possible values:

  - disabled: Source kept as original. This is a default.
  - inplace: removes new lines and tabs.
  - inplace_full: removes new lines and tabs but also those which were converted to strings (i.e. \n)
  - shrink: removes new lines and tabs, plus merging multiple spaces and blank lines to a single space.
  - shrink_full: remove new lines and tabs, including stringified values, plus shrinking spaces and blank lines.

  And here is the simplest form of how we use that.

  ```scala
  import com.johnsnowlabs.nlp._
  val documentAssembler = new DocumentAssembler()               
                             .setInputCol("text")     
                             .setOutputCol("document")     
                             .setCleanupMode("shrink")
  val doc_df = documentAssembler.transform(spark_df)
  ```

  

