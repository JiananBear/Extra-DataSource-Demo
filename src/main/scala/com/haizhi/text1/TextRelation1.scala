package com.haizhi.text1

import com.haizhi.{CusTextInputFormat, CusTextOutputFormat}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by luckybear on 16/8/12.
  */

class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, paths: Array[String], dataSchema: Option[StructType], partitionColumns: Option[StructType], parameters: Map[String, String]): HadoopFsRelation = {
    new TextRelation1(paths, parameters)(sqlContext)
  }
  override def shortName(): String = "bear"
}

class TextRelation1(override val paths: Array[String], parameters: Map[String, String] = Map.empty[String, String])(@transient val sqlContext: SQLContext) extends HadoopFsRelation {

  val tableSchema = parameters.getOrElse("table_schema", "value")

  override def dataSchema: StructType = {
    val structFields = tableSchema.split(",").map(one => new StructField(one, StringType, true))
    new StructType(structFields)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, context)
      }
    }
  }

  override def buildScan(inputFiles: Array[FileStatus]): RDD[Row] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths.map(new Path(_)): _*)
    }
    val rows = sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf], classOf[CusTextInputFormat], classOf[LongWritable], classOf[Text])
    rows.map[Row] { pair =>
      val values = pair._2.toString.split(",")
      Row(values: _*)
    }
  }
}

class TextOutputWriter(path: String, dataSchema: StructType, context: TaskAttemptContext) extends OutputWriter {

  private val recordWriter: RecordWriter[NullWritable, Row] = {
    new CusTextOutputFormat() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
        val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = {
    recordWriter.write(NullWritable.get(), row)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }
}
