package com.jd.bdp.fpmc.produce.offline

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by zhengchen on 2015/8/25.
 */
object TestHbase {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "BJHC-HBase-Magpie-17896.jd.local:2181," +
      "BJHC-HBase-Magpie-17895.jd.local:2181,BJHC-HBase-Magpie-17894.jd.local:2181," +
      "BJHC-HBase-Magpie-17893.jd.local:2181,BJHC-HBase-Magpie-17897.jd.local:2181")
    hbaseConf.set("zookeeper.znode.parent", "/hbase_han_river")


    val startTs = DateTime.now - 2.minutes
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "o2o_nearline_order_buyer_sum")
    hbaseConf.set(TableInputFormat.SCAN_TIMERANGE_START, startTs.getMillis.toString)

    val userItemActionRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).foreach { case (_,result) =>
        result.list().toArray.foreach { case kv: KeyValue =>
          println(Bytes.toString(kv.getQualifier()))
        }
      }

  }

}
