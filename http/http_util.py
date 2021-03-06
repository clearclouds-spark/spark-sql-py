#!/usr/bin/python
import os
import json
import urllib
from datetime import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
	

# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']
    
    
def dump_file(topic, output, info_type):
    base_path = "/home/juyun/datafile/json-file/live/peer1/"
    path = base_path + datetime.now().strftime("%Y/%m/%d/%H/")
    if not os.path.exists(path):
        mkdir_p(path)
    name = "nfcapd." + str(int(datetime.now().strftime("%Y%m%d%H%M%S"))/10 * 10) + "." + info_type + "_10s"
    print path+name
    with open(path+name, "w+") as f:
        json.dump(output, f, indent=4)
        
        
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5 (except OSError, exc: for Python <2.5)
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise
        
        
def kafka_spark_streaming_sql_main(app_name, brokers, topic, interval_seconds, sql_function):
    sc = SparkContext(appName=app_name)
    sqlContext = SQLContext(sc)
    #ssc = StreamingContext(sc, interval_seconds)
    ssc = StreamingContext(sc, 10)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    kvs.foreachRDD(sql_function)
    ssc.start()
    ssc.awaitTermination()
    

def json_rdd_to_sql_df(rdd):
     sqlContext = getSqlContextInstance(rdd.context)
     lines = rdd.map(lambda x: x[1])
     df = sqlContext.jsonRDD(lines)
     df.printSchema()
     return df


if __name__ == '__main__':
    pass
