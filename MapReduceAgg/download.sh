#!/bin/bash
## download data on HDFS

hadoop fs -mkdir /user/ubuntu/2020;

hadoop distcp \
-Dfs.s3a.endpoint=s3.amazonaws.com \
-Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \
s3a://nyc-tlc/trip\ data/yellow_tripdata_2020* 2020/;