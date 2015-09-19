package com.jd.bdp.fpmc.o2o.jobs

import com.github.nscala_time.time.Imports._
import com.jd.bdp.fpmc.{JobContext, Job}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by zhengchen on 2015/9/16.
 */

object O2oUserSkuFeatureTmpJob extends Job[SparkContext, JobContext] {

  def run(sc: SparkContext, jc: JobContext): Unit = {
    val sqlContext = new HiveContext(sc)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    val yesterday = jc.date.toString(fmt)
    val start_time3 = (jc.date - 2.days).toString(fmt)
    val start_time7 = (jc.date - 6.days).toString(fmt)
    val sql =
      s"""
        |insert overwrite table adm.o2o_user_sku_features_tmp
        |partition (dt='$yesterday',action='buy')
        |select
        |         order.buyer_pin,
        |         sku.id,
        |         sku.categoryid,
        |         sku.brandid,
        |         sku.shopcategories,
        |         sku.orgcode,
        |         sum(three_buy) as three_buy,
        |         sum(seven_buy) as seven_buy
        |from
        |        (
        |           select
        |                   order_id ,
        |                   buyer_pin,
        |                   case when substring(order_start_time,0,10)>= '$start_time3'
        |                   and substring(order_start_time,0,10)<='$yesterday'
        |                   then 1 else 0 end three_buy,
        |                   case when substring(order_start_time,0,10)>='$start_time7'
        |                   and substring(order_start_time,0,10)<='$yesterday'
        |                   then 1 else 0 end seven_buy
        |                   from
        |                          fdm.fdm_o2o_ocs_core_order_main_chain
        |                   where
        |                          dp='ACTIVE'
        |                          and yn = 0
        |                          and substring(order_start_time,0,10)>='$start_time7'
        |                          and substring(order_start_time,0,10)<='$yesterday'
        |        ) order
        |join
        |       (
        |          select
        |                   order_id,
        |                   sku_id
        |          from
        |                   fdm.fdm_o2o_ocs_core_order_product_chain
        |          where
        |                   dp='ACTIVE'
        |                   and yn = 0
        |                   and substring(create_time,0,10)>='$start_time7'
        |                   and substring(create_time,0,10)<='$yesterday'
        |        ) order_product
        |on
        |        order.order_id =cast(order_product.order_id as string)
        |join
        |       (
        |          select
        |                  id,
        |                  categoryid,
        |                  brandid,
        |                  shopcategories,
        |                  orgcode
        |          from
        |                  fdm.fdm_o2o_pms_sku_main_chain
        |          where
        |                  dp = 'ACTIVE'
        |		     and categoryid!=''
        |                  and brandid!=''
        |                  and shopcategories!=''
        |                  and shopcategories!='[]'
        |                  and orgcode is not null
        |       ) sku
        |on
        |      cast(order_product.sku_id as string) = sku.id
        |group by
        |          order.buyer_pin,
        |          sku.id,
        |          sku.categoryid,
        |          sku.brandid,
        |          sku.shopcategories,
        |          sku.orgcode
      """.stripMargin
    sqlContext.sql(sql)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val jc = JobContext()

  }

}
