FPMC
====

###Produce
hive -> DataFrame -> RDD[Features] -> **Hbase**

example:
```scala
  import com.jd.bdp.fpmc.entity.result.{FeaturesId, Feature, Features}
  import com.jd.bdp.fpmc.produce.offline.{SparkSql2HbaseWorkFlow, SparkSql2HbaseParams}
  import com.jd.bdp.fpmc.storage.HbaseStorage

  // Prepare hql, function which map dataFrame to Features and HbaseStorage instance.
  val params = SparkSql2HbaseParams("select sku,brand from item_table",
      _ => new Features(new FeatureID(_.getInt(0)).addFeature(new Feature("b", _.getInt(1))),
      new HbaseStorage("ZK-Host","parent","HbaseTable","Family"))
  
  // build SparkSql to Hbase work flow.
  val workflow = new SparkSql2HbaseWorkFlow(params)
  // Turn dataframe to RDD[Features]
  val sc = new SparkContext(new SparkConf)
  val featureRdd = workflow.data2feature(new HiveContext(sc))
  // Storage Features to Hbase
  workflow.feature2storage(featureRdd)
```

###Consume
hiveTable -> DataFrame -> RDD[Action] -> **RDD[Example]** <- Hbase

example:
```scala
  import com.jd.bdp.fpmc.consume.{HbaseReader, HbaseReaderParams}
  import com.jd.bdp.fpmc.entity.origin.Action
  import com.jd.bdp.fpmc.entity.result.FeaturesId
  import com.jd.bdp.fpmc.storage.HbaseStorage

  // Prepare hql, function which map dataFrame to Action and HbaseStorage instance.
  val params = HbaseReaderForSparkSqlParma("select label,sku from log",
    _ => new Action.setLabel(_.getInt(0)).addFsid(new FeaturesID(_.getInt(1)))
    new HbaseStorage("ZK-Host","parent","HbaseTable","Family"))

  // build Hbase Reader for SparkSql
  val reader = new HbaseReaderForSparkSql(params)
  // Combine the label and Features to generate examples
  reader.makeExamples
```

###Entity
Features

Action

Example