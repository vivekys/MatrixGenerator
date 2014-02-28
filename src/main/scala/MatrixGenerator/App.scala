package MatrixGenerator

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration

/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {
    val result = ToolRunner.run(new Configuration(), Generator, args)
    System.exit(result)
  }
}

