package com.luckybear

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

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Row] = {
    val conf = job.getConfiguration
    val extension = ""
    val file: Path = getDefaultWorkFile(job, extension)
    val fs: FileSystem = file.getFileSystem(conf)
    val out = fs.create(file)
    new CusLineRecordWriter(out)
  }
}


private[luckybear] class CusLineRecordWriter(out: DataOutputStream, keyValueSeparator: String = "\t") extends RecordWriter[NullWritable, Row] {

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

object CusLineRecordWriter {
  val utf8 = "UTF-8"
  val newLine = "\n".getBytes("UTF-8")


}
