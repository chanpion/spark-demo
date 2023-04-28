spark-submit \
    --class com.cord.StartApplication  \
    --executor-memory 4G \
    --num-executors 8 \
    --master yarn-client \
/data/cord/spark-example-1.0-SNAPSHOT.jar