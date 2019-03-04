stop-all.sh
hdfs namenode -format
hdfs datanode -format
start-all.sh
hdfs dfs -put /home/thyrix/Downloads/mapreduce_ks/cpp/data_1e9_20000.txt /_input
hadoop jar ~/Downloads/mapreduce_ks/topkapi/out/artifacts/topkapi_jar/topkapi.jar -input /_input -output /_output -CMS_l 5 -CMS_b 2048
