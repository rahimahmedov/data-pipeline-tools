#!/usr/bin/env bash
spark-submit --master yarn \
  --deploy-mode client \
  --class ra.etl.pipes.sparkstreaming.StreamingApp \
  kudu-insert-1.0-SNAPSHOT.jar etl_config_params.hocon
