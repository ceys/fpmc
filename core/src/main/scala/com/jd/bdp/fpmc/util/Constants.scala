package com.jd.bdp.fpmc.util

/**
 * Created by zhengchen on 2015/8/27.
 */
object Constants {

  // hbase
  val HBASE_ZOOKEEPER_QUORUM = "BJHC-HBase-Magpie-17896.jd.local:2181," +
    "BJHC-HBase-Magpie-17895.jd.local:2181,BJHC-HBase-Magpie-17894.jd.local:2181," +
    "BJHC-HBase-Magpie-17893.jd.local:2181,BJHC-HBase-Magpie-17897.jd.local:2181"

  val HBASE_ZOOKEEPER_ZNODE_PARENT = "/hbase_han_river"

  // hbase family
  val HBASE_FEATURE_OFFLINE_FAMILY = "d"

  // hbase table
  val HBASE_ITEM_FEATURE_TABLE = "o2o_item_feature"

  // vw namespace
  val VW_NAME_ITEM_CATEGORYID = "a"
  val VW_NAME_ITEM_BRANDID = "b"
  val VW_NAME_ITEM_SKUPRICE = "c"
  val VW_NAME_ITEM_SKUMARKETPRICE = "d"
  val VW_NAME_ITEM_LENGTH = "e"
  val VW_NAME_ITEM_WIDE = "f"
  val VW_NAME_ITEM_HIGH = "g"
  val VW_NAME_ITEM_WEIGHT = "h"
  val VW_NAME_ITEM_SHOPCATEGORIES = "i"
  val VW_NAME_ITEM_STOCKNUM = "j"
  val VW_NAME_ITEM_ORGCODE = "k"


}
