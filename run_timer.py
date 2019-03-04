import time
from subprocess import call

# call(["hdfs", "dfs", "-ls", "/"])
start_time = time.time()
# call(["hadoop", "jar", "/home/thyrix/Downloads/mapreduce_ks/topkapi/out/artifacts/topkapi_jar/topkapi.jar", "/input", "/output"])
call(["hadoop",  "jar",  "/home/thyrix/Software/hadoop-2.8.5/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.5.jar",
    "wordcount", "/input", "/output"])
print(time.time() - start_time)