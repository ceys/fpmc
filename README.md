FPMC
====

###Produce
hive -> DataFrame -> RDD[Features] -> **Hbase**

����˵����
date�����������ڣ���ʽΪ2015-09-22
sql: ��ȡ���ݵ�sql
ftype�� �������ͣ�cross,user,item��
mapping��sql row��feature��ӳ�䣬
  ����attrCateΪ������������ID����Ҫ��������
  user��ֵ��Ӧ��sql��user_id��������
  attrid��Ӧsql��id��������
  fIndexΪһ���ֵ䣬����vw�����ռ�����ֵ�Ƕ�Ӧsql�е�������
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

����˵����
sql����ȡ��ǩ���ݵ�sql
output�����������hdfs·��
mapping��sql row����ǩ��ӳ�䣬����
  user��Ӧsql���������û�id��������
  item��Ӧsql��������skuid��������Ҫ��bigint���ͣ���
  timestamp��ʱ�����Ӧ��sql��������������λΪs������Ϊint��
  label����ǩ��Ӧ��sql������������
  attrmap���������������ֵӳ�䣬��Ϊ�����������ͣ�ֵΪ����������Ӧ��sql����������
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