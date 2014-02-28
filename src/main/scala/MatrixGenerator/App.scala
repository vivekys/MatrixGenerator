package MatrixGenerator

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.orc.MatrixDump

/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {
    val conf = new Configuration()
    if (conf.get("orcfiledump") == "dump") {
      MatrixDump.dump(args)
    }
    else {
      val result = ToolRunner.run(new Configuration(), Generator, args)
      System.exit(result)
    }
  }
}

