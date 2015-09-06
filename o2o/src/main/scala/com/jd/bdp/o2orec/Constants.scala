package com.jd.bdp.o2orec

/**
 * Created by zhengchen on 2015/9/6.
 */
object Constants {

  // ================== vw namespace ================
  val  VW_NAMESPACE_3_DAYS_BUY_BRAND = "a"
  val  VW_NAMESPACE_7_DAYS_BUY_BRAND = "b"

  // ================= hbase config =====================
  val HBASE_ZOOKEEPER_QUORUM = "BJHC-HBase-Magpie-17896.jd.local:2181," +
    "BJHC-HBase-Magpie-17895.jd.local:2181,BJHC-HBase-Magpie-17894.jd.local:2181," +
    "BJHC-HBase-Magpie-17893.jd.local:2181,BJHC-HBase-Magpie-17897.jd.local:2181"

  val HBASE_ZOOKEEPER_ZNODE_PARENT = "/hbase_han_river"

  // ================= hbase table ===============
  val HBASE_USER_ITEM_CROSS_TABLE = "o2o_user_item_feature"

  // ================= hbase table family ===============
  val HBASE_OFFLINE_FEATURE_FAMILY = "d"

  // ================= cross features attr id ===========
  val CROSS_FEATURES_BRAND_ID: Char = 1

}
