package com.haizhi

import java.io.{EOFException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, _}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred._
import org.apache.hadoop.net.NetworkTopology

import scala.collection.mutable.ArrayBuffer

/**
  * Created by luckybear on 16/8/5.
  */
class CusTextInputFormat extends FileInputFormat[LongWritable, Text] {

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {
    reporter.setStatus(split.toString)
    new CusLineRecordReader(job, split.asInstanceOf[FileSplit])
  }

}


class CusLineRecordReader(job: Configuration, split: FileSplit) extends RecordReader[LongWritable, Text] {

  private val start: Long = split.getStart
  private var pos: Long = start
  private val end: Long = start + split.getLength
  private var temp: Array[Byte] = Array()
  private val fileIn: FSDataInputStream = {
    val path = split.getPath
    val fs = path.getFileSystem(job)
    fs.open(path)
  }
  private val CR: Byte = '\r'
  private val LF: Byte = '\n'

  //  private def getLastLineEndPos(in: FSDataInputStream): Long ={
  //    if (start > 0) {
  //      in.seek(start - 1)
  //      val last = in.readByte()
  //      if (last == CR || last == LF) {
  //        start
  //      } else {
  //        val len = 100
  //        val buffer = new Array[Byte](len)
  //        val tempPos = {
  //          if (start > 100) { start - 100 } else start
  //        }
  //        in.seek(start - 100)
  //
  //      }
  //    }
  //  }

  override def next(key: LongWritable, value: Text): Boolean = {
    key.set(pos)
    if (pos >= end) {
      return false
    }
    val needSkip = start != 0 && pos == start
    fileIn.seek(pos)
    var isEnd = false
    if (needSkip) {
      while (!isEnd && pos < end) {
        val b = fileIn.readByte()
        pos = fileIn.getPos
        if (b == CR || b == LF) {
          isEnd = true
        }
      }
      isEnd = false
    }
    val bytes = new ArrayBuffer[Byte]()
    // 获取行内容
    try {
      while (!isEnd && pos < end) {
        val b = fileIn.readByte()
        pos = fileIn.getPos
        if (b == CR || b == LF) {
          isEnd = true
        } else {
          bytes.append(b)
        }
      }
      if (isEnd) {
        value.set(bytes.toArray)
        println(value)
        true
      } else {
        try {
          while (!isEnd) {
            val b = fileIn.readByte()
            pos = fileIn.getPos
            if (b == CR || b == LF) {
              isEnd = true
            } else {
              bytes.append(b)
            }
          }
        } catch {
          case e:EOFException =>
        }

        value.set(bytes.toArray)
        println(value)
        true
        //        val buffer = Array(pos - 1 - startLinePos)
        //        val buffer = new Array[Byte]((pos - 1 - startLinePos).toInt)
        //        fileIn.read(buffer, 0, buffer.length)
        //        value.set(buffer)
      }
    } catch {
      case e: EOFException =>
        value.set(bytes.toArray)
        true
      case e: Exception =>
        false

    }

  }

  override def getProgress: Float = {
    if (start == end) {
      0.0f
    } else {
      Math.min(1.0f, (pos - start) / (end - start))
    }
  }

  override def getPos: Long = {
    pos
  }

  override def createKey(): LongWritable = {
    new LongWritable()
  }

  override def close(): Unit = {
    if (fileIn != null) {
      fileIn.close()
    }
  }

  override def createValue(): Text = {
    new Text()
  }
}
