package MatrixGenerator

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.Tool
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.util
import java.io.{IOException, DataOutput, DataInput}
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoUtils, TypeInfo, TypeInfoFactory}
import org.apache.hadoop.hive.ql.io.orc.{OrcNewOutputFormat, OrcSerde}
import scala.util.Random
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{LazyOutputFormat, MultipleOutputs, FileOutputFormat}

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
  private val NUM_COLF = "mapreduce.matrixgenerator.num-colF"

  def setNumRows(job : Job, rows : Int) {
    job.getConfiguration.setInt(NUM_ROWS, rows)
  }

  def setNumCols(job : Job, cols : Int) {
    job.getConfiguration.setInt(NUM_COLS, cols)
  }

  def setNumColF(job : Job, colF : Int) {
    job.getConfiguration.setInt(NUM_COLF, colF)
  }

  def getNumRows(job : JobContext) = job.getConfiguration.getInt(NUM_ROWS, 10)
  def getNumCols(job : JobContext) = job.getConfiguration.getInt(NUM_COLS, 10)
  def getNumColF(job : JobContext) = job.getConfiguration.getInt(NUM_COLF, 10)

  class GenMapper extends Mapper[IntWritable, NullWritable, NullWritable, Writable] {
    val serde = new OrcSerde()
    val rand = new Random()
    var oip : ObjectInspector = null
    var numColF : Int = 0
    var numRows : Int = 0
    var numCols : Int = 0
    var output : MultipleOutputs[NullWritable, Writable] = null
    type Context = Mapper[IntWritable, NullWritable, NullWritable, Writable]#Context

    override def setup(context : Context) {
      numColF = Generator.getNumColF(context)
      numCols = Generator.getNumCols(context) / numColF
      numRows = Generator.getNumRows(context)
      oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(SchemaGenerator.schemaGen(numCols))
      output = new MultipleOutputs[NullWritable, Writable](context)
    }


    override def map(key : IntWritable, value : NullWritable, context : Context) {
      for (i <- 1 to numColF) {
        val data = new util.ArrayList[Int](numCols)
        for (j <- 1 to numCols) {
          val d = rand.nextInt()
          data.add(d)
        }
        val row = serde.serialize(data, oip)
        output.write("cf"+i, NullWritable.get(), row)
      }
    }

    override def cleanup(context : Context) {
      output.close()
    }
  }

  def usage = println("matrixGen <num rows> <num cols> <num colf> <output dir>")

  //Initalize the MapReduce
  def run(args: Array[String]): Int = {
    val conf = getConf
    conf.set("mapred.job.map.memory.mb", "6144")
    val job = new Job(conf, "RandomMatrix Generator")
    if (args.length != 4) {
      usage
      return 2
    }
    setNumRows(job, args(0).toInt)
    setNumCols(job, args(1).toInt)
    setNumColF(job, args(2).toInt)
    val outputDir = new Path(args(3))

    if (outputDir.getFileSystem(getConf).exists(outputDir)) {
      throw new IOException("Output dir " + outputDir + " already exists")
    }
    FileOutputFormat.setOutputPath(job, outputDir)
//    job.setOutputFormatClass(classOf[OrcNewOutputFormat])
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[GenMapper])
    job.setNumReduceTasks(0)
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Writable])
    job.setInputFormatClass(classOf[RangeInputFormat])

    for (i <- 1 to Generator.getNumColF(job)) {
      MultipleOutputs.addNamedOutput(job, "cf"+i, classOf[OrcNewOutputFormat], classOf[NullWritable], classOf[Writable])
    }
    LazyOutputFormat.setOutputFormatClass(job, classOf[OrcNewOutputFormat])
    return if (job.waitForCompletion(true)) 0 else 1
  }

}
/**
 * An input format that assigns ranges of Ints to each mapper.
 */
class RangeInputFormat extends InputFormat[IntWritable, NullWritable] {
  override def getSplits(job: JobContext): util.List[InputSplit] = {
    val totalRows = Generator.getNumRows(job)
    val numSplits = 120
    val splits = new util.ArrayList[InputSplit]()
    val splitList = 1 to totalRows by numSplits
    for (split <- splitList) {
      val endRow = if (split + numSplits < totalRows)
                      split + numSplits
                   else
                      totalRows
      splits.add(new RangeInputSplit(split, endRow))
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

object SchemaGenerator {
  def schemaGen (numCols : Int) : TypeInfo = {
    val colNames = new util.ArrayList[String](numCols)
    val colTypes = new util.ArrayList[TypeInfo](numCols)
    val intType = TypeInfoFactory.getPrimitiveTypeInfo("int")
    val prefix = "c-"
    for (i <- 1 to numCols) {
      val colName = prefix + i
      colNames.add(colName)
      colTypes.add(intType)
    }
    TypeInfoFactory.getStructTypeInfo(colNames, colTypes)
  }
}
