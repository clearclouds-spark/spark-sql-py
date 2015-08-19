import sys
import os
import json
import urllib
from datetime import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def extract_host(url):
    if url is None:
        pass
    proto, rest = urllib.splittype('http://'+url)
    host, rest = urllib.splithost(rest)
    host, port = urllib.splitport(host)
    if port is None:
        port = 80
    return host


# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def dump_file(topic, output):
    path = topic + '/' + datetime.now().strftime("%Y/%m/%d/%H/")
    if not os.path.exists(path):
        mkdir_p(path)
    name = topic + '.' + datetime.now().strftime("%Y%m%d%H%M%S")
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


def process(time, rdd):
    #try:
    print "========= %s =========" % str(time)
    if rdd.count() > 0:
        sqlContext = getSqlContextInstance(rdd.context)
        lines = rdd.map(lambda x: x[1])
        df = sqlContext.jsonRDD(lines)
        df.printSchema()
        total_groups = df.select("dst_group_id").distinct().dropna().collect()
        # Register this DataFrame as a table.
        get_host = udf(extract_host)
        df.select('*', get_host(df['url']).alias("host")).registerAsTable("http")
        urls = sqlContext.sql("SELECT dst_group_id, host, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as in_bytes, sum(out_bytes) as out_bytes, sum(latency_sec * 1000000 + latency_usec) as latency, count(*) as requests FROM http where url is not null group by dst_group_id, host")
        urls.registerAsTable("urls")
        output = {}
        for opt in ['total_bytes', 'in_bytes', 'out_bytes', 'latency', 'requests']:
            output[opt] = {}
            for group_id in total_groups:
                topN_sql = "select host, total_bytes, in_bytes, out_bytes, latency, requests from urls where dst_group_id = %s order by %s desc limit 50" % (group_id.dst_group_id, opt)
                topN_collect = sqlContext.sql(topN_sql).toJSON().collect()
                output[opt][group_id.dst_group_id] = list(json.loads(x) for x in topN_collect) 
        dump_file("http", output)
    #except:
       # print "error"
       # pass


if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 5)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #zkQuorum, topic = sys.argv[1:]
    #kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 3})
    kvs.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
    
