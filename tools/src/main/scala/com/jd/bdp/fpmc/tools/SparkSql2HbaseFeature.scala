package com.jd.bdp.fpmc.tools

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.entity.result._
import com.jd.bdp.fpmc.produce.offline.{SparkSql2Hbase, SparkSql2HbaseParams}
import com.jd.bdp.fpmc.storage.HbaseStorage
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser
import grizzled.slf4j.Logging
import spray.json._
import MyJsonProtocol._

/**
 * Created by zhengchen on 2015/9/17.
 */
object SparkSql2HbaseFeature extends Logging {

  case class SparkSql2HbaseFeatureConfig(
                                date: String = "",
                                sql: String = "",
                                mapping: String = "",
                                ftype: String = ""
                                )

  val parser = new OptionParser[SparkSql2HbaseFeatureConfig]("FeatureMeta") {
    opt[String]("date") action {(g, c) =>
      c.copy(date = g)
    } text "date of features"
    opt[String]("sql") action {(g, c) =>
      c.copy(sql = g)
    } text "sql that extract row data"
    opt[String]("mapping") action {(g, c) =>
      c.copy(mapping = g)
    } text "json string tha map row data to features"
    opt[String]("ftype") action {(g, c) =>
      c.copy(ftype = g)
    } text "feature types: user / item / cross"
  }

  case class CrossFeatureMapping(user: Int, attrid: Int, attrCate: Int, fIndex: Map[String, Int])
  case class OneFeatureMapping(id: Int, fIndex: Map[String, Int])

  def main(args: Array[String]): Unit = {

    val fmcOpt = parser.parse(args, SparkSql2HbaseFeatureConfig())
    if (fmcOpt.isEmpty) {
      logger.error("SparkSql2HbaseFeatureConfig is empty. Quitting")
      return
    }

    val fmc = fmcOpt.get
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val timeStamp = (fmt.parseMillis(fmc.date) / 1000).toInt

    val mappingFunc: Row => Features = fmc.ftype match {
      case Constants.FTYPE_CROSS =>
        val mapping = fmc.mapping.parseJson.convertTo[CrossFeatureMapping]
        row => {
          val id = new CrossFeaturesId(row.getString(mapping.user),
            mapping.attrCate,
            row.getLong(mapping.attrid),
            timeStamp,
            true)
          val result = new Features(id)
          mapping.fIndex.map { case (k: String, v: Int) => result.addFeature(new Feature(k, row.getString(v))) }
          result
        }
      case Constants.FTYPE_USER =>
        val mapping = fmc.mapping.parseJson.convertTo[OneFeatureMapping]
        row: Row => {
          val id = new UserFeaturesId(row.getString(mapping.id), timeStamp, true)
          val result = new Features(id)
          mapping.fIndex.map { case (k: String, v: Int) => result.addFeature(new Feature(k, row.getString(v))) }
          result
        }
      case Constants.FTYPE_ITEM =>
        val mapping = fmc.mapping.parseJson.convertTo[OneFeatureMapping]
        row: Row => {
          val id = new ItemFeaturesId(row.getLong(mapping.id), timeStamp, true)
          val result = new Features(id)
          mapping.fIndex.map { case (k: String, v: Int) => result.addFeature(new Feature(k, row.getString(v))) }
          result
        }
    }

    val hbaseStorage: HbaseStorage = fmc.ftype match {
      case Constants.FTYPE_CROSS => new HbaseStorage( Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_ITEM_CROSS_TABLE
        , Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))
      case Constants.FTYPE_USER => new HbaseStorage( Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_USER_FEATURE_TABLE
        , Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))
      case Constants.FTYPE_ITEM => new HbaseStorage( Constants.HBASE_ZOOKEEPER_QUORUM,
        Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_ITEM_FEATURE_TABLE
        , Bytes.toBytes(Constants.HBASE_OFFLINE_FEATURE_FAMILY))
    }

    val params = SparkSql2HbaseParams(fmc.sql, mappingFunc, hbaseStorage)

    val workflow = new SparkSql2Hbase(params)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val featureRdd = workflow.data2feature(sqlContext)
    workflow.feature2storage(featureRdd)
    sc.stop()
  }

}
