import sys
import json
from pyspark.sql.functions import udf
from http_util import *


def get_http_filter_status(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        status_info = df.groupby("status_code").count().toJSON().collect()   
        output = {}
        for s in status_info:
            d_s = json.loads(s)
            output[d_s['status_code']] = d_s['count']
        dump_file("http", output, "http_filter_status")
    except Exception as e:
        print e     
    
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpFilterStatus", brokers, topic, 5, get_http_filter_status)

