### What is this folder?

To add a new experiment, add object a new `<YourExperimentName>Starter.scala`

```scala
package convenience

import runners.LocalStarter

object ExperiementStarter {
  val experiment = "Experiement"
  val inputDataPath = "data/<YourInputData>"
  val outputDataPath = "topicOutput/<yourExpermentName>Output"
  val utilsDataPath = "utilDictionaries/<yourUtilDataFile>.json"

  def main(args: Array[String])
  : Unit = {
    LocalStarter.main(Array(experiment, inputDataPath, outputDataPath, utilsDataPath))
  }
}
```
