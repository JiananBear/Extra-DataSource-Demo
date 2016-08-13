package com.haizhi.text

import com.haizhi.CusTextInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by luckybear on 16/8/3.
  */

class DefaultSource extends RelationProvider with DataSourceRegister {
  override def shortName(): String = "bdptext"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new TextRelation(parameters)(sqlContext)
  }
}


class TextRelation(parameters: Map[String, String] = Map.empty[String, String])
                   (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  val tableSchema = parameters.getOrElse("table_schema", sys.error("not valid schema"))
  val pathStr = parameters.getOrElse("path", sys.error("not valid schema"))
  val paths = {
    if (pathStr.nonEmpty) {
      Option(pathStr.split(",").map(path => new Path(path)))
    } else {
      Option(null)
    }
  }
  //  override val schema: StructType = textSchema.getOrElse(new StructType().add("value", StringType))

  override val schema: StructType = {
    val structFields = tableSchema.split(",").map(one => new StructField(one, StringType, true))
    new StructType(structFields)
  }

  override def buildScan(): RDD[Row] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths.get: _*)
    }
    val rows = sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf], classOf[CusTextInputFormat], classOf[LongWritable], classOf[Text])
    rows.map[Row] { pair =>
      val values = pair._2.toString.split(",")
      Row(values: _*)
    }
  }
}



