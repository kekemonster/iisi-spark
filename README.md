# iisi-spark
### Teaching materials for the demonstration on 2016-08-31

#### Softwares
1. jdk-8u102-linux-x64.tar.gz
2. scala-2.11.8.tgz
3. spark-2.0.0-bin-hadoop2.7.tgz
4. hadoop-2.7.3.tar.gz
5. sbt-0.13.12.zip

#### Spark-shell
```bash
spark-shell --master local[2]
spark-shell --master spark://spark-node1:7077
spark-shell --master yarn --deploy-mode client
```

#### Web UIs
| Service | URL                      |
|---------|--------------------------|
| HDFS    | http://spark-node1:50070 |
| YARN    | http://spark-node1:8088  |
| Spark   | http://spark-node1:4040  |
