package com.jd.bdp.fpmc.tools

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.consume.{HbaseReader, HbaseReaderParams}
import com.jd.bdp.fpmc.o2o.Constants
import com.jd.bdp.fpmc.storage.HbaseStorage
import grizzled.slf4j.Logging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

/**
 * Created by zhengchen on 2015/9/19.
 */
object MakeExamples  extends Logging {

  case class MakeExamplesConfig(
                                          mapping: String = "",
                                          sql: String = "",
                                          features: String = "",
                                          output: String = ""
                                          )

  val parser = new OptionParser[MakeExamplesConfig]("FeatureMeta") {
    opt[String]("date") action {(g, c) =>
      c.copy(mapping = g)
    } text "json string that map row to label format"
    opt[String]("sqlfile") action {(g, c) =>
      c.copy(sql = g)
    } text "sql that extract row data"
    opt[String]("features") action {(g, c) =>
      c.copy(features = g)
    } text "json string about the features to be used"
    opt[String]("ftype") action {(g, c) =>
      c.copy(output = g)
    } text "the hdfs path that store the example"
  }

  def main(args: Array[String]): Unit = {
    val fmcOpt = parser.parse(args, MakeExamplesConfig())
    if (fmcOpt.isEmpty) {
      logger.error("MakeExamplesConfig is empty. Quitting")
      return
    }
    val fmc = fmcOpt.get

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val DATE = (DateTime.now - 1.days).toString(fmt)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    Array(Constants.HBASE_USER_ITEM_CROSS_TABLE,
      Constants.HBASE_USER_ITEM_CROSS_TABLE,
      Constants.HBASE_USER_ITEM_CROSS_TABLE).foreach

    val hbaseStorage = new HbaseStorage(Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_ITEM_CROSS_TABLE,
      Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))

    val readerParams = new HbaseReaderParams(fmc.sql, click2Action, hbaseStorage)
    val reader = new HbaseReader(readerParams)

    reader.makeExamples(sqlContext).map(_.toVW).saveAsTextFile(s"/tmp/zc/fpmc/example/$DATE")
    sc.stop()
  }

}
