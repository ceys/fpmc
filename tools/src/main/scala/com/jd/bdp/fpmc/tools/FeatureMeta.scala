package com.jd.bdp.fpmc.tools

import com.jd.bdp.fpmc.FeatureNameAlreadyUsedException
import com.jd.bdp.fpmc.entity.origin.FeatureDescription
import com.jd.bdp.fpmc.storage.HbaseTable
import org.apache.hadoop.hbase.util.Bytes
import scopt.OptionParser
import spray.json._
import MyJsonProtocol._
import grizzled.slf4j.Logging

import scala.io.Source

/**
 * Created by zhengchen on 2015/9/14.
 */
object FeatureMeta extends Logging {

  case class FeatureMetaConfig(
                              showAll: Boolean = false,
                              featureJsonFile: String = ""
                                )

  val parser = new OptionParser[FeatureMetaConfig]("FeatureMeta") {
    opt[Unit]("showall") action {(_, c) =>
      c.copy(showAll = true)
    } text "shows all features meta data"
    opt[String]("push") action {(g, c) =>
      c.copy(featureJsonFile = g)
    } text "feature`s information json file to be pushed to meta data"
  }

  def main(args: Array[String]): Unit = {
    val fmcOpt = parser.parse(args, FeatureMetaConfig())
    if (fmcOpt.isEmpty) {
      logger.error("WorkflowConfig is empty. Quitting")
      return
    }

    val fmc = fmcOpt.get
    if (fmc.showAll) {
      val metaTable = new HbaseTable(Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_META_DATA_TABLE,
      Bytes.toBytes(Constants.HBASE_TABLE_DEFAULT_FAMILY))
      val featureMap = metaTable.getAllFeatureMeta(Bytes.toBytes(Constants.HBASE_FEATURE_META_ROWKEY))
      featureMap.foreach {case(name: String, dsp: FeatureDescription) =>
        println(dsp.toJson.prettyPrint)
      }
    } else if (fmc.featureJsonFile != "") {
      val featureJson = Source.fromFile(fmc.featureJsonFile).mkString.parseJson

      val featureDescription = featureJson.convertTo[FeatureDescription]
      val metaTable = new HbaseTable(Constants.HBASE_ZOOKEEPER_QUORUM,
      Constants.HBASE_ZOOKEEPER_ZNODE_PARENT, Constants.HBASE_META_DATA_TABLE,
      Bytes.toBytes(Constants.HBASE_TABLE_DEFAULT_FAMILY))
      try {
        metaTable.pushFeatureMeta(featureDescription, Bytes.toBytes(Constants.HBASE_FEATURE_META_ROWKEY))
      } catch {
        case fnaue: FeatureNameAlreadyUsedException =>
          println(fnaue.getMessage)
          return
      }
    }
  }


}
