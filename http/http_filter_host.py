import sys
import json
from pyspark.sql.functions import udf
from http_util import *


def extract_host(url):
    if url is None:
        pass
    proto, rest = urllib.splittype('http://'+url)
    host, rest = urllib.splithost(rest)
    host, port = urllib.splitport(host)
    if port is None:
        port = 80
    return host


def get_http_filter_host(time, rdd):
    try:
        print "========= %s =========" % str(time)
        sqlContext = getSqlContextInstance(rdd.context)
        df = json_rdd_to_sql_df(rdd)
        get_host = udf(extract_host)
        df.select('*', get_host(df['url']).alias("host")).registerAsTable("http")
    
        hosts_info = sqlContext.sql("""SELECT host,
                                 sum(in_bytes + out_bytes) as total_bytes, 
                                 avg(in_bytes) as in_bytes, 
                                 avg(out_bytes) as out_bytes, 
                                 avg(latency_sec * 1000000 + latency_usec) as latency,
                                 count(*) as requests 
                                 FROM http group by host""")
        output = {}
        for opt in ['total_bytes', 'in_bytes', 'out_bytes', 'latency', 'requests']:
            output[opt] = list(json.loads(x) for x in hosts_info.sort(hosts_info[opt].desc()).limit(50).toJSON().collect())
     
        dump_file("http", output, "http_filter_host")
    except Exception as e:
        print e     
    
    
if __name__ == "__main__":
    brokers, topic = sys.argv[1:]
    kafka_spark_streaming_sql_main("HttpFilterHost", brokers, topic, 5, get_http_filter_host)

