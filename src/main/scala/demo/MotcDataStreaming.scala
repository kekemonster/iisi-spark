package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object MotcDataStreaming {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(10));
    
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
    lines
      .map(x => parse(x))
      .filter(x => x.status == 0 && x.errTp.equals("diag0"))
      .map(x => (x.vdId + "," + x.datacollecttime + "," + x.vsrId + "," + x.vsrDir, (x, 1)))
      .reduceByKey(summation)
      .map(mapOutput)
      .print()
    
    //nc -lk 9999
    ssc.start()
    ssc.awaitTermination()
  }
  
  case class MotcData(
    vdId: String,
    status: Int,
    vsrDir: Int,
    vsrId: Int,
    speed: Int,
    sVolume: Int,
    tVolume: Int,
    lVolume: Int,
    mVolume: Int,
    occ: Int,
    errTp: String,
    datacollecttime: String)
  def parse(line:String) = {
    val splits = line.split('\t').transform(x => x.trim)
    MotcData(
      splits(0),
      splits(1).toInt,
      splits(3).toInt,
      splits(4).toInt,
      splits(5).toInt,
      Math.max(splits(6).toInt, 0),
      Math.max(splits(7).toInt, 0),
      Math.max(splits(8).toInt, 0),
      Math.max(splits(9).toInt, 0),
      splits(10).toInt,
      splits(11), splits(14).substring(0, 13))
  }
  def summation(x:(MotcData,Int),y:(MotcData,Int)) = {
    (MotcData(
      x._1.vdId,
      x._1.status,
      x._1.vsrDir,
      x._1.vsrId,
      x._1.speed + y._1.speed * (y._1.sVolume + y._1.tVolume + y._1.lVolume + y._1.mVolume),
      x._1.sVolume + y._1.sVolume,
      x._1.tVolume + y._1.tVolume,
      x._1.lVolume + y._1.lVolume,
      x._1.mVolume + y._1.mVolume,
      x._1.occ + y._1.occ,
      x._1.errTp,
      x._1.datacollecttime), x._2 + y._2)
  }
  def mapOutput(x:(String,(MotcData,Int))) = {
    val data = x._2._1;
    val cnt = x._2._2;
    val sumVolume = data.sVolume + data.tVolume + data.lVolume + data.mVolume;
    (x._1,
      cnt,
      1.0 * data.occ / cnt,
      if (sumVolume > 0) 1.0 * data.speed / sumVolume else 0,
      data.lVolume,
      Math.floor(60.0 * data.lVolume / cnt),
      data.sVolume,
      Math.floor(60.0 * data.sVolume / cnt),
      data.mVolume,
      Math.floor(60.0 * data.mVolume / cnt),
      data.tVolume,
      Math.floor(60.0 * data.tVolume / cnt),
      sumVolume,
      Math.floor(60.0 * sumVolume / cnt))
  }
}