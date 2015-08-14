import os
import sys
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, when 

STATIC_URL_TYPE =  ["jpg","png","gif","ico","js","css","txt","pdf","pptx","docx","rar","zip","tar","jar"]

if __name__ == "__main__":
    sc = SparkContext(appName="HttpPythonSQL")
    sqlContext = SQLContext(sc)
    if len(sys.argv) < 2:
        path = "file://" + os.path.join(os.environ['SPARK_HOME'], "examples/src/main/resources/people.json")
    else:
        path = sys.argv[1]
    # Create a DataFrame from the file(s) pointed to by path
    df = sqlContext.read.json(path).cache()
  
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
    with open("http_filter_result.json", "w") as f:
        json.dump(output, f, indent=4)
    sc.stop()
