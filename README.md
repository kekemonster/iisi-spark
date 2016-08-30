# iisi-spark
### Teaching materials for the demonstration on 2016-08-31

#### Softwares
1. jdk-8u102-linux-x64.tar.gz
2. spark-2.0.0-bin-hadoop2.7.tgz
3. hadoop-2.7.3.tar.gz

#### Web UIs
| Service | URL                      |
|---------|--------------------------|
| HDFS    | http://spark-node1:50070 |
| YARN    | http://spark-node1:8088  |
| Spark   | http://spark-node1:8080  |

#### Spark execution modes
```bash
spark-shell --master local[N]
spark-shell --master spark://spark-node1:7077
spark-shell --master yarn --deploy-mode client
```

#### Benchmarks
| Mode             | time      |
|------------------|-----------|
| yarn             | 6m16.001s |
| spark-local[2]   | 4m11.887s |
| spark-local[*]   | 2m31.806s |
| spark-standalone | 1m22.155s |
| spark-yarn       | 1m37.983s |

##### Bechmark commands
```bash
#yarn
hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount /user/hadoop/vbig.txt /user/hadoop/out
#spark-local[N]
spark-submit \
  --class demo.WordCount \
  --master local[*] \
  /home/spark-wordcount.jar hdfs://spark-node1:9000/user/hadoop/vbig.txt hdfs://spark-node1:9000/user/hadoop/out
#spark-standalone
spark-submit \
  --class demo.WordCount \
  --master spark://spark-node1:7077 \
  /home/spark-wordcount.jar hdfs://spark-node1:9000/user/hadoop/vbig.txt hdfs://spark-node1:9000/user/hadoop/out
#spark-yarn
spark-submit \
  --class demo.WordCount \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 512m \
  --executor-memory 1g \
  --num-executors 2 \
  --executor-cores 4 \
  /home/spark-wordcount.jar hdfs://spark-node1:9000/user/hadoop/vbig.txt hdfs://spark-node1:9000/user/hadoop/out
```