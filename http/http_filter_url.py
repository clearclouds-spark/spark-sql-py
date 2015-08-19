import sys
import json
from pyspark.sql.functions import udf
from http_util import *


def get_http_filter_url(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        df.registerAsTable("http")

        http_info = sqlContext.sql("""SELECT url,
                                 sum(in_bytes + out_bytes) as total_bytes, 
                                 avg(in_bytes) as in_bytes, 
                                 avg(out_bytes) as out_bytes, 
                                 avg(latency_sec * 1000000 + latency_usec) as latency,
                                 count(*) as requests 
                                 FROM http group by url""")
        output = {}
        for opt in ['total_bytes', 'in_bytes', 'out_bytes', 'latency', 'requests']:
            output[opt] = list(json.loads(x) for x in http_info.sort(http_info[opt].desc()).limit(50).toJSON().collect())
     
        dump_file("http", output, "http_filter_url")
    except Exception as e:
        print e     
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpFilterUrl", brokers, topic, 5, get_http_filter_url)

