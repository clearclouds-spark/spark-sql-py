import sys
import json
from pyspark.sql.functions import udf
from http_util import *


STATIC_URL_TYPE =  ["jpg","png","gif","ico","js","css","txt","pdf","pptx","docx","rar","zip","tar","jar"]

def get_http(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        total_groups = df.select("dst_group_id").distinct().dropna().collect()
        df.registerAsTable("http")
    
        urls = sqlContext.sql("SELECT dst_group_id, url, sum(in_bytes + out_bytes) as total_bytes, sum(in_bytes) as in_bytes, sum(out_bytes) as out_bytes, sum(latency_sec * 1000000 + latency_usec) as latency, count(*) as requests FROM http where url is not null group by dst_group_id, url")
        
        urls.registerAsTable("urls")
        output = {}
        #for url.split('/')[-1].split('.')[-1] in STATIC_URL_TYPE:
        for group_id in total_groups:
            topN_sql = "select url, total_bytes, in_bytes, out_bytes, latency, requests from urls where dst_group_id = %s" % (group_id.dst_group_id)
            topN_collect = sqlContext.sql(topN_sql).toJSON().collect()
            output[group_id.dst_group_id] = list(json.loads(x) for x in topN_collect)
        dump_file("http", output, "http")
    except Exception as e:
        print e     
    
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("Http", brokers, topic, 5, get_http)

