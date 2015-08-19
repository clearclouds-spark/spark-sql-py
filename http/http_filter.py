import sys
import json
from pyspark.sql.functions import udf
from http_util import *


STATIC_URL_TYPE =  ["jpg","png","gif","ico","js","css","txt","pdf","pptx","docx","rar","zip","tar","jar"]

def get_http_filter(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        url_type = udf(lambda url: url.split('/')[-1].split('.')[-1])
        df.select('*', when(url_type(df['url']).inSet(STATIC_URL_TYPE), 1).otherwise(0).alias("url_type")).registerAsTable("http")
        url_info = sqlContext.sql("""SELECT url_type, 
                                          avg(in_bytes) as in_bytes, 
                                          avg(out_bytes) as out_bytes, 
                                          avg(latency_sec) as latency_sec,
                                          avg(latency_usec) as latency_usec, 
                                          count(*) as requests 
                                          FROM http group by url_type""").toJSON().collect()
        output = {}
        for info in url_info:
            temp = json.loads(info)
            if temp['url_type'] == 1:
                output['static_request'] = temp
            else:
                output['dynamic_request'] = temp
        dump_file("http", output, "http_filter_url")
    except Exception as e:
        print e     
    
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpFilter", brokers, topic, 5, get_http_filter)

