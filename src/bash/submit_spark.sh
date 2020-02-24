#!/usr/bin/env bash
spark-submit --master yarn --deploy-mode cluster --queue root.stream --class com.azercell.bigdata.sparkstreaming.EPGSparkKafkaStream --principal dloper@AZERCELL.COM --keytab dloper.keytab kudu-insert-1.0-SNAPSHOT.jar -KAFKA_GROUP_ID attemptK002 -KAFKA_STREAMING_TIME_IN_SECONDS 15

spark-submit --master yarn --deploy-mode client  --class com.azercell.bigdata.sparkstreaming.GenericStreamingApp --principal dloper@AZERCELL.COM --keytab dloper.keytab kudu-insert-1.0-SNAPSHOT.jar hdfs://nameservice1/user/dloper/etl_config_prod.hocon