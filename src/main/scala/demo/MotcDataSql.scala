package demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object MotcDataSql {
  
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(this.getClass.getName).config(conf).getOrCreate()
    
    val struct = StructType(
      StructField("vd_id", DataTypes.StringType, false) ::
      StructField("status", DataTypes.IntegerType, false) ::
      StructField("vsr_dir", DataTypes.IntegerType, false) ::
      StructField("vsr_id", DataTypes.IntegerType, false) ::
      StructField("speed", DataTypes.IntegerType, false) ::
      StructField("s_volume", DataTypes.IntegerType, false) ::
      StructField("t_volume", DataTypes.IntegerType, false) ::
      StructField("l_volume", DataTypes.IntegerType, false) ::
      StructField("m_volume", DataTypes.IntegerType, false) ::
      StructField("occ", DataTypes.IntegerType, false) ::
      StructField("err_tp", DataTypes.StringType, true) ::
      StructField("datacollecttime", DataTypes.StringType, false) :: Nil
    )

    //the most straight-forward way is to load the csv directly,
    //however, data type transformation required for the schema defined...
    //val df = spark.read.option("sep", "\t").schema(struct).csv("hdfs://spark-node1:9000/user/hadoop/motc/*")

    //create a RDD, transform it, and then convert to table
    val files = sc.textFile(inputFile)
    val rowRdd = files.map(x => x.split("\t").transform(x => x.trim))
      .map(x =>
        Row(x(0), x(1).toInt, x(3).toInt, x(4).toInt, x(5).toInt,
          x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt,
          x(11), x(14)))
    spark.createDataFrame(rowRdd, struct).createOrReplaceTempView("motc")
    
    //SQL doing hourly aggregation and then save the output into file
    spark.sql(
      "SELECT vd_id, data_collect_hour, vsr_id, vsr_dir, " +
      "       data_count, occ, speed, " +
      "       l_volume, FLOOR(l_volume/data_count*60) fixed_l_volume, " +
      "       s_volume, FLOOR(s_volume/data_count*60) fixed_s_volume, " +
      "       m_volume, FLOOR(m_volume/data_count*60) fixed_m_volume, " +
      "       t_volume, FLOOR(t_volume/data_count*60) fixed_t_volume, " +
      "       l_volume+s_volume+m_volume+t_volume sum_volume, "+
      "       FLOOR((l_volume+s_volume+m_volume+t_volume)/data_count*60) fixed_sum_volume" +
      "  FROM ( " +
      "    SELECT vd_id, data_collect_hour, vsr_id, vsr_dir, " +
      "           COUNT(*) data_count, AVG(occ) occ, " +
      "           SUM(speed * (l_volume + s_volume + m_volume + t_volume)) / SUM(l_volume + s_volume + m_volume + t_volume) speed, " +
      "           SUM(l_volume) l_volume, SUM(s_volume) s_volume, SUM(m_volume) m_volume, SUM(t_volume) t_volume " +
      "      FROM ( " +
      "        SELECT vd_id, SUBSTR(datacollecttime, 1, 13) data_collect_hour, vsr_id, vsr_dir, " +
      "               occ, speed, " +
      "               IF(l_volume >= 0, l_volume, 0) l_volume, " +
      "               IF(s_volume >= 0, s_volume, 0) s_volume, " +
      "               IF(m_volume >= 0, m_volume, 0) m_volume, " +
      "               IF(t_volume >= 0, t_volume, 0) t_volume " +
      "          FROM motc " +
      "         WHERE status = 0 and err_tp = 'diag0' " +
      "      ) a " +
      "     GROUP BY vd_id, data_collect_hour, vsr_id, vsr_dir " +
      "  ) a " +
      "  ORDER BY vd_id, vsr_id, data_collect_hour ").rdd.map(x => x.mkString(",")).saveAsTextFile(outputFile)
  }
  
}