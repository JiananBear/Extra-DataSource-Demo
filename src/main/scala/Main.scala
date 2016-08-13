import java.io.StringWriter
import java.util.UUID

import com.haizhi.CusTextOutputFormat
import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by luckybear on 16/8/4.
  */
object Main {
  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("dataSource")
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("dataSource")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("com.haizhi.text").option("table_schema", "name,age,sex").load("hdfs://localhost:9000/bdp/test/test")
    df.printSchema()
    df.registerTempTable("heheda")
    val textDf = sqlContext.sql("select * from heheda")
    textDf.show()

    val jsonDDL =
      s"""
         |CREATE TEMPORARY TABLE gaga
         |USING com.haizhi.text1
         |OPTIONS (
         | path  'hdfs://localhost:9000/bdp/test/test',
         | table_schema 'name,age,sex'
         |)""".stripMargin

    sqlContext.sql(jsonDDL)
    val text1Df = sqlContext.sql("select * from gaga")
    text1Df.show()


    text1Df.write.format("com.haizhi.text1").option("table_schema", "name,age,sex").save("hdfs://localhost:9000/bdp/test/mytest2")
    //    hc.sql("create table test1 stored as  as select * from heheda")
    //    hc.sql("select * from test1").show()
    df.rdd.map(row =>
      (NullWritable.get(), row)
    ).saveAsNewAPIHadoopFile("hdfs://localhost:9000/bdp/test/heheda1", classOf[NullWritable], classOf[Row], classOf[CusTextOutputFormat])
  }
}

