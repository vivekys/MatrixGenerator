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
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, LazyOutputFormat, MultipleOutputs, FileOutputFormat}

/**
 * Created with IntelliJ IDEA.
 * User: vives
 * Date: 22/02/14
 * Time: 6:39 PM
 * To change this template use File | Settings | File Templates.
 */
object TextGenerator extends Configured with Tool {
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

  class GenMapper extends Mapper[IntWritable, NullWritable, Text, Text] {
    val rand = new Random()
    var numColF : Int = 0
    var numRows : Int = 0
    var numCols : Int = 0
    var output : MultipleOutputs[Text, Text] = null
    type Context = Mapper[IntWritable, NullWritable, Text, Text]#Context

    val data = new Text()
    val rowKey = new Text()
    val dataStr = new StringBuilder
    val rowKeyStr = new StringBuilder

    override def setup(context : Context) {
      numColF = TextGenerator.getNumColF(context)
      numCols = TextGenerator.getNumCols(context) / numColF
      numRows = TextGenerator.getNumRows(context)
      output = new MultipleOutputs[Text, Text](context)
    }

    override def map(key : IntWritable, value : NullWritable, context : Context) {
      for (i <- 1 to numColF) {
        data.clear()
        dataStr.clear()
        rowKeyStr.clear()
        rowKey.clear()
        for (j <- 1 to numCols) {
          val d = rand.nextInt()
          //          val d = (key.get().toString + ((i - 1) * numColF + j).toString).toInt
          if (j > 1)
            dataStr.append("\t").append(d)
          else
            dataStr.append(d)
        }
        rowKeyStr.append(key.get()).append("-").append(((i-1)*numCols)+1)

        rowKey.append(rowKeyStr.toString().getBytes("utf-8"), 0, rowKeyStr.toString().length)
        data.append(dataStr.toString().getBytes("utf-8"), 0, dataStr.toString().length)

        output.write("cf"+i, rowKey, data)
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
    //job.setOutputFormatClass(classOf[OrcNewOutputFormat])
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[GenMapper])
    job.setNumReduceTasks(0)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    job.setInputFormatClass(classOf[RangeInputFormat])

    for (i <- 1 to TextGenerator.getNumColF(job)) {
      MultipleOutputs.addNamedOutput(job, "cf"+i, classOf[TextOutputFormat[Text, Text]], classOf[Text], classOf[Text])
    }
    LazyOutputFormat.setOutputFormatClass(job, classOf[TextOutputFormat[Text, Text]])
    return if (job.waitForCompletion(true)) 0 else 1
  }

}
