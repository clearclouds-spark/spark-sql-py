import sys
import json
from http_util import *

  
def get_http_url(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        total_groups = df.select("dst_group_id").distinct().dropna().collect()  
        # Register this DataFrame as a table.
        df.registerAsTable("http")
        urls = sqlContext.sql("""SELECT dst_group_id, url, 
            sum(in_bytes + out_bytes) as bytes_total, 
            sum(in_bytes) as bytes_in, 
            sum(out_bytes) as bytes_out, 
            sum(latency_sec * 1000000 + latency_usec) as latency, 
            count(*) as requests FROM http where url is not null group by dst_group_id, url""")
        urls.registerAsTable("urls")
        output = {}
        for opt in ['bytes_total', 'bytes_in', 'bytes_out', 'latency', 'requests']:
            output[opt] = {}
            for group_id in total_groups:
                topN_sql = "select url, bytes_total, bytes_in, bytes_out, latency, requests from urls where dst_group_id = %s order by %s desc limit 50" % (group_id.dst_group_id, opt)
                topN_collect = sqlContext.sql(topN_sql).toJSON().collect()
                output[opt][group_id.dst_group_id] = list(json.loads(x) for x in topN_collect) 
        dump_file("http", output, "http_url")
    except Exception as e:
        print e     
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpUrl", brokers, topic, 5, get_http_url)

