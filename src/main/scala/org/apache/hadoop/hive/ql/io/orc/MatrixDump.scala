package org.apache.hadoop.hive.ql.io.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConversions._

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
      System.out.println("Type: " + reader.getObjectInspector.getTypeName)
      System.out.println("\nStripe Statistics:")
      val  metadata = reader.getMetadata
      for (n <- 0 until metadata.getStripeStatistics.size) {
        println("  Stripe " + (n + 1) + ":")
        val ss = metadata.getStripeStatistics.get(n)
        for (i <- 0 until ss.getColumnStatistics.length) {
          println("    Column " + i + ": " + ss.getColumnStatistics.toList(i))
        }
      }
      val stats = reader.getStatistics.toList
      println("\nFile Statistics:")
      for (i <- stats) {
        System.out.println("  Column " + i + ": " + i)
      }
      println("\nStripes:")
      while (reader.getStripes.iterator.hasNext) {
        val stripe = reader.getStripes.iterator.next
        val stripeStart = stripe.getOffset
        println("  Stripe: " + stripe)
        val footer = rows.readStripeFooter(stripe)
        var sectionStart = stripeStart
        for(section <- footer.getStreamsList) {
          println("    Stream: column " + section.getColumn +
            " section " + section.getKind + " start: " + sectionStart +
            " length " + section.getLength)
          sectionStart += section.getLength
        }
        for (i <- 0 until footer.getColumnsCount) {
          val encoding = footer.getColumns(i)
          val buf = new StringBuilder()
          buf.append("    Encoding column ")
          buf.append(i)
          buf.append(": ")
          buf.append(encoding.getKind)
          if (encoding.getKind == OrcProto.ColumnEncoding.Kind.DICTIONARY) {
            buf.append("[")
            buf.append(encoding.getDictionarySize)
            buf.append("]")
          }
          println(buf)
        }
      }
      rows.close();
    }
  }
}
