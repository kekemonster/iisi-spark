package demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    
    sc.textFile(inputFile)
      .flatMap(x => x.split(" ")) //flatMap(_.split(" ")
      .map(x => (x, 1)) //map((_, 1))
      .reduceByKey((x, y) => x + y) //reduceByKey(_+_)
      .saveAsTextFile(outputFile)
  }
  
}