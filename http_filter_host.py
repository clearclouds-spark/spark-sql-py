import os
import sys
import json
import urllib
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, when 


def extract_host(url):
    if url is None:
        pass
    proto, rest = urllib.splittype('http://'+url)
    host, rest = urllib.splithost(rest)
    host, port = urllib.splitport(host)
    if port is None:
        port = 80
    return host


if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)
    if len(sys.argv) < 2:
        path = "file://" + os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    df = sqlContext.read.json(path).cache()
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
 
    with open("http_filter_host_result.json", "w") as f:
        json.dump(output, f, indent=4)
    sc.stop()