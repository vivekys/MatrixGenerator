package org.apache.hadoop.hive.ql.io.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

/**
 * Created with IntelliJ IDEA.
 * User: vives
 * Date: 28/02/14
 * Time: 9:06 PM
 * To change this template use File | Settings | File Templates.
 */
object MatrixDump {

  def dump(args : Array[String]) {
    val conf = new Configuration()
    for(filename <- args) {
      println("Structure for " + filename)
      val path = new Path(filename)
      val reader = OrcFile.createReader(path.getFileSystem(conf), path, conf)
      val rows = reader.rows(null).asInstanceOf[RecordReaderImpl]
      println("Rows: " + reader.getNumberOfRows)
      println("Compression: " + reader.getCompression)
      if (reader.getCompression != CompressionKind.NONE) {
        System.out.println("Compression size: " + reader.getCompressionSize)
      }
      println("Type: " + reader.getObjectInspector.getTypeName)

      val soi = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
      println(soi)
      rows.close();
    }
  }
}
