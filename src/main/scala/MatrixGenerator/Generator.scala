package MatrixGenerator

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.Tool
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.util
import java.io.{IOException, DataOutput, DataInput}
import org.apache.hadoop.mapred.{JobConf}
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoUtils, TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.ql.io.orc.{OrcNewOutputFormat, OrcSerde}
import scala.util.Random
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Created with IntelliJ IDEA.
 * User: vives
 * Date: 22/02/14
 * Time: 6:39 PM
 * To change this template use File | Settings | File Templates.
 */
object Generator extends Configured with Tool {
  private val NUM_ROWS = "mapreduce.matrixgenerator.num-rows"
  private val NUM_COLS = "mapreduce.matrixgenerator.num-cols"

  def setNumRows(job : Job, rows : Long) {
    job.getConfiguration.setLong(NUM_ROWS, rows)
  }

  def setNumCols(job : Job, cols : Long) {
    job.getConfiguration.setLong(NUM_COLS, cols)
  }

  def getNumRows(job : JobContext) = job.getConfiguration.getLong(NUM_ROWS, 10)
  def getNumCols(job : JobContext) = job.getConfiguration.getLong(NUM_COLS, 10)

  class GenMapper extends Mapper[LongWritable, NullWritable, Object, Writable] {
    val serde = new OrcSerde()
    val rand = new Random()
    var oip : ObjectInspector = null
    var numCols : Int = 0
    var numRows : Int = 0
    type Context = Mapper[LongWritable, NullWritable, Object, Writable]#Context

    override def setup(context : Context) {
      numCols = Generator.getNumCols(context).asInstanceOf[Int]
      numRows = Generator.getNumRows(context).asInstanceOf[Int]
      oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(SchemaGenerator.schemaGen(numCols))
    }


    override def map(key : LongWritable, value : NullWritable, context : Context) {
      val rowId = key.get()
      val bitsToShiftLeft = Math.ceil(Math.log(numRows)).asInstanceOf[Int]
      val data = new util.ArrayList[Long](numCols.asInstanceOf[Int])
      for (i <- 0 until numCols) {
        val d = rand.nextLong().<<(bitsToShiftLeft) + rowId
        System.out.println("Generating row : " + d + " for rowID : " + rowId)
        data.add(d)
      }
      val row = serde.serialize(data, oip)
      context.write(null, row)
    }
  }

  def usage = println("matrixGen <num rows> <num cols> <output dir>")

  //Initalize the MapReduce
  def run(args: Array[String]): Int = {
    val job = new Job(getConf, "RandomMatrix Generator")
    if (args.length != 3) {
      usage
      return 2
    }
    setNumRows(job, args(0).toLong)
    setNumCols(job, args(1).toLong)
    val outputDir = new Path(args(2))

    if (outputDir.getFileSystem(getConf).exists(outputDir)) {
      throw new IOException("Output dir " + outputDir + " already exists")
    }
    FileOutputFormat.setOutputPath(job, outputDir)
    job.setOutputFormatClass(classOf[OrcNewOutputFormat])
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[GenMapper])
    job.setNumReduceTasks(0)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Writable])
    job.setInputFormatClass(classOf[RangeInputFormat])
    return if (job.waitForCompletion(true)) 0 else 1
  }

}
/**
 * An input format that assigns ranges of longs to each mapper.
 */
class RangeInputFormat extends InputFormat[LongWritable, NullWritable] {
  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val totalRows = Generator.getNumRows(job)
    val numSplits = job.getConfiguration.asInstanceOf[JobConf].getNumMapTasks;
    val splits = new util.ArrayList[InputSplit]()
    var currentRow = 0L
    for (split <- 0 until numSplits) {
      val goal = Math.ceil(totalRows * (split + 1).asInstanceOf[Double] / numSplits).asInstanceOf[Long]
      splits.add(new RangeInputSplit(currentRow, goal - currentRow))
      currentRow = goal
    }
    splits
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, NullWritable] = {
    new RangeRecordReader
  }
}

/**
 * An input split consisting of a range on numbers.
 */
class RangeInputSplit(var firstRow : Long = 0, var rowCount : Long = 0) extends InputSplit with Writable {

  def this () {this(0, 0)}

  override def readFields(in: DataInput) = {
    firstRow = WritableUtils.readVLong(in);
    rowCount = WritableUtils.readVLong(in);
  }

  override def write(out: DataOutput) {
    WritableUtils.writeVLong(out, firstRow);
    WritableUtils.writeVLong(out, rowCount);
  }

  override def getLength: Long = 0

  override def getLocations: Array[String] = Array.empty
}

/**
 * A record reader that will generate a range of numbers.
 */
class RangeRecordReader extends RecordReader[LongWritable, NullWritable] {
  var startRow = 0L
  var finishedRows = 0L
  var totalRows = 0L
  var key : LongWritable = null

  override def close() {
  //Nothing to close
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext) {
    startRow = split.asInstanceOf[RangeInputSplit].firstRow
    totalRows = split.asInstanceOf[RangeInputSplit].rowCount
  }

  override def nextKeyValue(): Boolean = {
    if (key == null) {
      key = new LongWritable()
    }

    if (finishedRows < totalRows) {
      key.set(startRow + finishedRows)
      finishedRows += 1;
      true
    } else
      false
  }

  override def getCurrentKey: LongWritable = key

  override def getCurrentValue: NullWritable = NullWritable.get()

  override def getProgress: Float = finishedRows/totalRows.asInstanceOf[Float]
}

object SchemaGenerator {
  def schemaGen (numCols : Int) : TypeInfo = {
    val colNames = new util.ArrayList[String](numCols)
    val colTypes = new util.ArrayList[TypeInfo](numCols)
    val intType = TypeInfoFactory.getPrimitiveTypeInfo("bigint")
    val prefix = "col-"
    for (i <- 1 to numCols) {
      val colName = prefix + i
      colNames.add(colName)
      colTypes.add(intType)
    }
    TypeInfoFactory.getStructTypeInfo(colNames, colTypes)
  }
}
