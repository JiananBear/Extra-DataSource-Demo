package com.haizhi

import java.io.DataOutputStream

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.sql.Row

/**
  * Created by luckybear on 16/8/8.
  */
class CusTextOutputFormat extends FileOutputFormat[NullWritable, Row] {

  //  override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable): RecordWriter[LongWritable, Row] = {
  //    val file = FileOutputFormat.getTaskOutputPath(job, name)
  //    val fs = file.getFileSystem(job)
  //    val fileOut = fs.create(file, progress)
  //    new CusLineRecordWriter(fileOut)
  //  }
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Row] = {
    val conf = job.getConfiguration
    val extension = ""
    val file: Path = getDefaultWorkFile(job, extension)
    val fs: FileSystem = file.getFileSystem(conf)
    val out = fs.create(file)
    new CusLineRecordWriter(out)
  }
}


private[haizhi] class CusLineRecordWriter(out: DataOutputStream, keyValueSeparator: String = "\t") extends RecordWriter[NullWritable, Row] {

  override def write(key: NullWritable, value: Row): Unit = {
    val nullValue = value == null || value.isInstanceOf[NullWritable]
    if (!nullValue) {
      val temp = value.mkString(",")
      writeObject(temp)
      out.write(CusLineRecordWriter.newLine)
    }
  }

  private def writeObject(value: Any): Unit = {
    out.write(value.toString.getBytes(CusLineRecordWriter.utf8))
  }

  override def close(context: TaskAttemptContext): Unit = {
    out.close()
  }
}

//private[haizhi] class CusLineRecordWriter(val out: DataOutputStream) extends RecordWriter[LongWritable, Row] {
//  override def write(key: LongWritable, value: Row): Unit = {
//    val nullValue = value == null || value.isInstanceOf[NullWritable]
//    if (!nullValue) {
////      for (i <- 0 until value.length) {
////        val temp = value.get(i)
////      }
//      val temp = value.mkString(",")
//      writeObject(temp)
//      out.write(CusLineRecordWriter.newLine)
//    }
//  }
//
////  override def close(reporter: Reporter): Unit = {
////    out.close()
////  }
//
//  private def writeObject(value: Any): Unit = {
//    out.write(value.toString.getBytes(CusLineRecordWriter.utf8))
//  }
//
//}
//
object CusLineRecordWriter {
  val utf8 = "UTF-8"
  val newLine = "\n".getBytes("UTF-8")


}
