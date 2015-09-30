package com.jd.bdp.fpmc.tools

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.consume.{HbaseReader, HbaseReaderParams}
import com.jd.bdp.fpmc.entity.origin.Label
import com.jd.bdp.fpmc.entity.result._
import com.jd.bdp.fpmc.storage.HbaseStorage
import grizzled.slf4j.Logging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser
import spray.json._
import MyJsonProtocol._

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
    opt[String]("mapping") action {(g, c) =>
      c.copy(mapping = g)
    } text "json string that map row to label format"
    opt[String]("sql") action {(g, c) =>
      c.copy(sql = g)
    } text "sql that extract row data"
    opt[String]("features") action {(g, c) =>
      c.copy(features = g)
    } text "json string about the features to be used"
    opt[String]("output") action {(g, c) =>
      c.copy(output = g)
    } text "the hdfs path that store the example"
  }

  case class LabelMapping(label: Int, user: Int, item: Int, attrmap: Map[String, Int], timestamp: Int)

  def main(args: Array[String]): Unit = {
    val fmcOpt = parser.parse(args, MakeExamplesConfig())
    if (fmcOpt.isEmpty) {
      logger.error("MakeExamplesConfig is empty. Quitting")
      return
    }
    val fmc = fmcOpt.get

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val DATE = (DateTime.now - 1.days).toString(fmt)

    val strgMap = Map(Constants.FTYPE_CROSS ->  new HbaseStorage(Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_ITEM_CROSS_TABLE,
        Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY)),
      Constants.FTYPE_ITEM -> new HbaseStorage(Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_ITEM_FEATURE_TABLE,
        Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY)),
      Constants.FTYPE_USER -> new HbaseStorage(Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_FEATURE_TABLE,
        Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY)))

    def row2Label(row: Row): Label = {
      val mapping = fmc.mapping.parseJson.convertTo[LabelMapping]
      val idMap: Map[String, Array[FeaturesID]] =
        Map(Constants.FTYPE_CROSS -> mapping.attrmap.map { case(attrtype: String, attrid: Int) =>
          new CrossFeaturesId(row.getString(mapping.user),
            attrtype.toInt,
            row.getLong(attrid),
            row.getInt(mapping.timestamp), true)
        }.toArray,
        Constants.FTYPE_ITEM -> Array(new ItemFeaturesId(row.getLong(mapping.item),
          row.getInt(mapping.timestamp), true)),
        Constants.FTYPE_USER -> Array(new UserFeaturesId(row.getString(mapping.user),
          row.getInt(mapping.timestamp), true)))
      Label(row.getInt(mapping.label), "", idMap)
    }

    var fSet = Set[String]()
    if (fmc.features != "") {
      fSet = fmc.features.parseJson.convertTo[Set[String]]
    }
    val readerParams = new HbaseReaderParams(fmc.sql, row2Label, strgMap, fSet)
    val reader = new HbaseReader(readerParams)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    reader.makeExamples(sqlContext).map(_.toVW).saveAsTextFile(fmc.output)
    sc.stop()
  }

}
