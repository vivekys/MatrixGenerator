package org.apache.hadoop.hive.ql.io.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector
import org.apache.hadoop.hive.ql.io.orc.OrcStruct.OrcStructInspector

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
        println("Compression size: " + reader.getCompressionSize)
      }
      println("Type: " + reader.getObjectInspector.getTypeName)

      val soi = reader.getObjectInspector.asInstanceOf[OrcStructInspector]
      val sb = new StringBuilder
      val fields = soi.getAllStructFieldRefs

      var row : OrcStruct = rows.next(null).asInstanceOf[OrcStruct]
      while (rows.hasNext) {
        row = rows.next(row).asInstanceOf[OrcStruct]
        for (i <- 0 until fields.size) {
          if (i > 0) {
            sb.append(",");
          }
          sb.append(row.getFieldValue(i)).append("\t")
//          sb.append(fields.get(i).getFieldObjectInspector().asInstanceOf[WritableLongObjectInspector].get(row));
        }
      }
      println(sb)
      rows.close();
    }
  }
}
