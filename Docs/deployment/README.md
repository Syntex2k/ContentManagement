### Java Version

Spark on the HTW-Server runs on Java 8. Therefore we have to make sure that we compile with that Java version. On OSX (and most likely Linux) you can switch the active Java with something like that: 

```bash
$ export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
```

### Spark Submit

```bash
$ spark2-submit --packages com.johnsnowlabs.nlp:spark-nlp_2.11:2.5.2 \
  --master yarn \ 
  --num-executors 24 \
  --class runners.HdfsStarter \
  --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle \
  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-8-oracle \
  scalanlp_2.11-0.1.jar \
  IndicatorAnalysis \
  cleanedBodyContentFull \
  indicatorOutput \
  utilDictionaries \
  models \
  s0563263
```

### SSH File System Mount

Connect to the HTW-Network using a VPN-Client (Cicsco).

Login to haadop HTW server:

```
$ ssh s0563413@hadoop05.f4.htw-berlin.de
```

Create excahnge folder on server:

```bash
$ mkdir exchange
```

Go to exchange folder on your loacal machine:

```bash
$ cd /Users/matthiastitze/UniHTW/FunkProg2/Repository/exchange
```

Mount remote exchange folder onto local exchnage folder:

```bash
sshfs s0563413@hadoop05.f4.htw-berlin.de:exchange .
```
