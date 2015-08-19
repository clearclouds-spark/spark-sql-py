import sys
import json
from http_util import *


def get_http_server(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        urls = sqlContext.sql("""SELECT dst_group_id, dst_ipv4, 
            sum(in_bytes + out_bytes) as total_bytes, 
            sum(in_bytes) as in_bytes, 
            sum(out_bytes) as out_bytes, 
            sum(latency_sec * 1000000 + latency_usec) as latency, 
            count(*) as requests 
            FROM %s where url is not null 
            group by dst_group_id, dst_ipv4""" % table_name)   
        urls.registerAsTable("urls")
        output = {}
        for opt in ['total_bytes', 'in_bytes', 'out_bytes', 'latency', 'requests']:
            output[opt] = {}
            for group_id in total_groups:
                topN_sql = """select dst_ipv4, total_bytes, in_bytes, out_bytes, latency, requests 
                    from urls where dst_group_id = %s order by %s desc limit 50""" % (group_id.dst_group_id, opt)
                topN_collect = sqlContext.sql(topN_sql).toJSON().collect()
                output[opt][group_id.dst_group_id] = list(json.loads(x) for x in topN_collect)
        dump_file("http", output, "http_server")
    except Exception as e:
        print e   
    

if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpServer", brokers, topic, 5, get_http_host)
    
