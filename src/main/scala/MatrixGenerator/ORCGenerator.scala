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
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: vives
 * Date: 22/02/14
 * Time: 6:39 PM
 * To change this template use File | Settings | File Templates.
 */
object ORCGenerator extends Configured with Tool {
  private val NUM_ROWS = "mapreduce.matrixgenerator.num-rows"
  private val NUM_COLS = "mapreduce.matrixgenerator.num-cols"
  private val NUM_COLF = "mapreduce.matrixgenerator.num-colF"
  private val NUM_ROWF = "mapreduce.matrixgenerator.num-colF"

  def setNumRows(job : Job, rows : Int) {
    job.getConfiguration.setInt(NUM_ROWS, rows)
  }

  def setNumCols(job : Job, cols : Int) {
    job.getConfiguration.setInt(NUM_COLS, cols)
  }

  def setNumColF(job : Job, colF : Int) {
    job.getConfiguration.setInt(NUM_COLF, colF)
  }

  def setNumRowF(job : Job, rowF : Int) {
    job.getConfiguration.setInt(NUM_ROWF, rowF)
  }

  def getNumRows(job : JobContext) = job.getConfiguration.getInt(NUM_ROWS, 10)
  def getNumCols(job : JobContext) = job.getConfiguration.getInt(NUM_COLS, 10)
  def getNumColF(job : JobContext) = job.getConfiguration.getInt(NUM_COLF, 10)
  def getNumRowF(job : JobContext) = job.getConfiguration.getInt(NUM_ROWF, 10)

  class GenMapper extends Mapper[IntWritable, NullWritable, NullWritable, Writable] {
    val serde = new OrcSerde()
    val rand = new Random()
    var oip : ObjectInspector = null
    var numColF : Int = 0
    var numRows : Int = 0
    var numCols : Int = 0
    var output : MultipleOutputs[NullWritable, Writable] = null
    var data : ArrayBuffer[Int] = null
    type Context = Mapper[IntWritable, NullWritable, NullWritable, Writable]#Context

    override def setup(context : Context) {
      numColF = ORCGenerator.getNumColF(context)
      numCols = ORCGenerator.getNumCols(context) / numColF
      numRows = ORCGenerator.getNumRows(context)
      oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(ORCSchemaGenerator.schemaGen(numCols))
      output = new MultipleOutputs[NullWritable, Writable](context)
      data = new ArrayBuffer[Int](numCols)
    }


    override def map(key : IntWritable, value : NullWritable, context : Context) {
      for (i <- 1 to numColF) {
        for (j <- 0 until numCols) {
//          val d = (key.get().toString + ((i - 1) * numColF + j).toString).toInt
          data(j) = rand.nextInt()
        }
        output.write("cf"+i, NullWritable.get(), serde.serialize(data, oip))
      }
    }

    override def cleanup(context : Context) {
      output.close()
    }
  }

  def usage = println("matrixGen <num rows> <num cols> <num rowf> <num colf> <output dir>")

  //Initalize the MapReduce
  def run(args: Array[String]): Int = {
    val conf = getConf
    conf.set("mapred.job.map.memory.mb", "6144")
    val job = new Job(conf, "RandomMatrix Generator")
    if (args.length != 5) {
      usage
      return 2
    }
    setNumRows(job, args(0).toInt)
    setNumCols(job, args(1).toInt)
    setNumRowF(job, args(2).toInt)
    setNumColF(job, args(3).toInt)
    val outputDir = new Path(args(4))

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

    for (i <- 1 to ORCGenerator.getNumColF(job)) {
      MultipleOutputs.addNamedOutput(job, "cf"+i, classOf[OrcNewOutputFormat], classOf[NullWritable], classOf[Writable])
    }
    LazyOutputFormat.setOutputFormatClass(job, classOf[OrcNewOutputFormat])
    return if (job.waitForCompletion(true)) 0 else 1
  }

}

object ORCSchemaGenerator {
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
