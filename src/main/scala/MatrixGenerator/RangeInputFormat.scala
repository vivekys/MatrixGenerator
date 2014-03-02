package MatrixGenerator

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.{WritableUtils, Writable, NullWritable, IntWritable}
import java.util
import java.io.{DataOutput, DataInput}

/**
 * Created with IntelliJ IDEA.
 * User: vives
 * Date: 02/03/14
 * Time: 12:27 PM
 * To change this template use File | Settings | File Templates.
 */
/**
 * An input format that assigns ranges of Ints to each mapper.
 */
class RangeInputFormat extends InputFormat[IntWritable, NullWritable] {
  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val totalRows = ORCGenerator.getNumRows(job)
    val numSplits = ORCGenerator.getNumRowF(job)

    val splits = new util.ArrayList[InputSplit]()
    val splitList = 1 to totalRows by numSplits
    for (split <- splitList) {
      val endRow = if (split + numSplits < totalRows)
        split + numSplits
      else
        totalRows
      splits.add(new RangeInputSplit(split, endRow-split))
    }
    splits
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[IntWritable, NullWritable] = {
    new RangeRecordReader
  }
}

/**
 * An input split consisting of a range on numbers.
 */
class RangeInputSplit(var firstRow : Int = 0, var rowCount : Int = 0) extends InputSplit with Writable {

  def this () {this(0, 0)}

  override def readFields(in: DataInput) = {
    firstRow = WritableUtils.readVInt(in);
    rowCount = WritableUtils.readVInt(in);
  }

  override def write(out: DataOutput) {
    WritableUtils.writeVInt(out, firstRow);
    WritableUtils.writeVInt(out, rowCount);
  }

  override def getLength: Long = 0

  override def getLocations: Array[String] = Array.empty
}

/**
 * A record reader that will generate a range of numbers.
 */
class RangeRecordReader extends RecordReader[IntWritable, NullWritable] {
  var startRow = 0
  var finishedRows = 0
  var totalRows = 0
  var key : IntWritable = null

  override def close() {
    //Nothing to close
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext) {
    startRow = split.asInstanceOf[RangeInputSplit].firstRow
    totalRows = split.asInstanceOf[RangeInputSplit].rowCount
  }

  override def nextKeyValue(): Boolean = {
    if (key == null) {
      key = new IntWritable()
    }

    if (finishedRows < totalRows) {
      key.set(startRow + finishedRows)
      finishedRows += 1;
      true
    } else
      false
  }

  override def getCurrentKey: IntWritable = key

  override def getCurrentValue: NullWritable = NullWritable.get()

  override def getProgress: Float = finishedRows/totalRows.asInstanceOf[Float]
}
