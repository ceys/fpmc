FPMC
====

###Produce
hive -> DataFrame -> RDD[Features] -> **Hbase**

参数说明：
date：特征的日期，格式为2015-09-22
sql: 提取数据的sql
ftype： 特征类型（cross,user,item）
mapping：sql row到feature的映射，
  其中attrCate为交叉特征类型ID，需要是整数，
  user的值对应的sql中user_id的索引，
  attrid对应sql中id的索引，
  fIndex为一个字典，键是vw命名空间名，值是对应sql中的索引。
example:
```
spark-submit \                                                                                                                                                                                                   
   --master local \                                                                                                                                                                                                 
   --num-executors 1 \                                                                                                                                                                                              
   --driver-memory 1g \                                                                                                                                                                                             
   --executor-memory 1g \                                                                                                                                                                                           
   --executor-cores 1 \                                                                                                                                                                                             
   --name test-zhengchen \                                                                                                                                                                                          
   --queue bdp_jmart_jdmp \                                                                                                                                                                                         
   --class com.jd.bdp.fpmc.tools.SparkSql2HbaseFeature \                                                                                                                                                            
   tools-0.1-SNAPSHOT.jar \                                                                                                                                                                                         
   --date $YESTERDAY \                                                                                                                                                                                              
   --sql "select user_id,cast(sku_id as bigint),cast(sum(three_days) as string),cast(sum(seven_days) as string) from adm.o2o_user_sku_features_tmp where dt='$YESTERDAY' and action='buy' group by user_id,sku_id" \
   --ftype cross \                                                                                                                                                                                                  
   --mapping '{"attrCate":1,"user":0,"attrid":1,"fIndex":{"ub3b":2,"ub7b":3}}' \
```

###Consume
hiveTable -> DataFrame -> RDD[Action] -> **RDD[Example]** <- Hbase

参数说明：
sql：提取标签数据的sql
output：样本输出的hdfs路径
mapping：sql row到标签的映射，其中
  user对应sql行数据中用户id的索引，
  item对应sql行数据中skuid的索引（要求bigint类型），
  timestamp：时间戳对应的sql行数据索引，单位为s，类型为int，
  label：标签对应的sql行数据索引，
  attrmap：交叉特征类别与值映射，建为交叉特征类型，值为交叉特征对应的sql行数据索引
example:
```
spark-submit \
   --master local \
   --num-executors 1 \
   --driver-memory 1g \
   --executor-memory 1g \
   --executor-cores 1 \
   --name test-zhengchen \
   --queue bdp_jmart_jdmp \
   --class com.jd.bdp.fpmc.tools.MakeExamples \
   tools-0.1-SNAPSHOT.jar \
   --sql "select user_log_acct, sku_id, request_time_sec, 1 from gdm.gdm_m14_online_o2o where dt='$YESTERDAY' and ct_page in ('detail' ,'GoodsInfo') and sku_id is not null and user_log_acct is not null and user_l
   og_acct != '' and refer_page in ('home','Home')" \
   --mapping '{"user":0,"item":1,"timestamp":2,"label":3,"attrmap":{"1":1}}' \
   --output /tmp/fpmc/example
```

###Entity
Features

Action

Example