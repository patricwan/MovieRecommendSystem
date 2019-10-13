package com.java.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple4;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;

import java.util.*;

public class StreamingRecommender {

    static int MAX_USER_RATINGS_NUM = 20;
    static int MAX_SIM_MOVIES_NUM = 20;
    static String MONGODB_STREAM_RECS_COLLECTION = "StreamRecs";
    static String MONGODB_RATING_COLLECTION = "Rating";
    static String MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs";

    public static void main(String[] args) throws Exception {
        System.out.println("This is the streaming Recommender Start");

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");


        SparkConf sparkConf = new SparkConf().setMaster((String) configMap.get("spark.cores")).setAppName("OfflineRecommender");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        //SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(15));

      /* Dataset<Row> dataset = spark.read().option("uri", (String)configMap.get("mongo.uri")).option("collection", MONGODB_MOVIE_RECS_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load().select("mid", "recs");
        dataset.printSchema();
        dataset.show();*/
        /*
        +------+--------------------+
|   mid|                recs|
+------+--------------------+
|  1859|[[111443, 0.96240...|
|    19|[[2379, 0.7782706...|
| 26371|[[25940, 1.0], [2...|
|  2227|[[1384, 1.0000000...|
| 30803|[[2905, 0.9287111...|
|101283|[[31000, 0.999999...|
|   307|[[306, 0.95801938...|
|  1907|[[2125, 0.8690336...|
|  2643|[[25971, 0.792003...|
|104595|[[1152, 1.0000000...|
|  2739|[[1674, 0.9084339...|
|  1923|[[152, 0.84939786...|
|  1523|[[2852, 0.9490219...|
|  2051|[[1011, 0.9670338...|
|102819|[[105731, 0.97437...|
         */

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.1.1.95:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "consumeGrpSpark");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("recommender");

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        Encoder<Tuple4<Integer, Integer, Double, Long>> tupleEncoder = Encoders.tuple(Encoders.INT(), Encoders.INT(), Encoders.DOUBLE(), Encoders.LONG());
        JavaDStream<Tuple4<Integer, Integer, Double, Long>> mappedRDD = kafkaStream.map(row -> {
            String[] splittedArr = row.value().split("\\|");
            if (splittedArr.length == 4) {
                return new Tuple4<Integer, Integer, Double, Long>(Integer.parseInt(splittedArr[0]), Integer.parseInt(splittedArr[1]),
                        Double.parseDouble(splittedArr[2]), Long.parseLong(splittedArr[3]));
            } else {
                return new Tuple4<Integer, Integer, Double, Long>(1, 2, 0.2, 3L);
            }
        });

        mappedRDD.print();

        mappedRDD.foreachRDD(rddBatch -> {
              JavaRDD<String>  eachMapped= rddBatch.map(eachRecord->{
                     return eachRecord._1() + "_" +eachRecord._2();
                });
              System.out.println("count " + eachMapped.count());
              rddBatch.foreach(eachRecord ->{
                System.out.println("eachRecord" + eachRecord);
                //System.out.println("eachRecord 1 " + eachRecord._1());
                  //Streaming recommendation
                  List<String> values = getUserRecentlyRating(10, eachRecord._1(), new Jedis("10.1.1.100", 6379));




              });

              // Get the singleton instance of SparkSession
              SparkSession spark = SparkSession.builder().config(rddBatch.context().getConf()).getOrCreate();
              //http://spark.apache.org/docs/2.3.0/streaming-programming-guide.html and SQL Dataframes
             //https://github.com/apache/spark/blob/v2.3.0/examples/src/main/java/org/apache/spark/examples/streaming/JavaSqlNetworkWordCount.java
             JavaRDD<StreamData> streamDataRDD = rddBatch.map(reachRecord-> {
                StreamData streamData = new StreamData();
                streamData.setUid(reachRecord._1());
                streamData.setMid(reachRecord._2());
                streamData.setScore(reachRecord._3());
                streamData.setLogTime(reachRecord._4());
                return streamData;
            });

            Dataset<Row> dataFrame = spark.createDataFrame(streamDataRDD, StreamData.class);

            // Creates a temporary view using the DataFrame
            dataFrame.createOrReplaceTempView("userItemsScores");

            Dataset<Row> userAvgScore =
                    spark.sql("select mid, avg(score) as avgScore from userItemsScores group by mid");
            userAvgScore.show();

        });

        //Sliding window function Test
        //param2: Windows length
        //param3: sliding length
        JavaDStream<Tuple4<Integer, Integer, Double, Long>>  reducedWindowRDD = mappedRDD.reduceByWindow((record1,record2)-> {
            return new Tuple4<Integer, Integer, Double, Long>(record1._1(), record2._2(), record1._3() + record2._3(), record2._4());
        },Durations.seconds(60),Durations.seconds(15));
        reducedWindowRDD.print();

        //Merge RDDS within this time window
        JavaDStream<Tuple4<Integer, Integer, Double, Long>> windowRDD = mappedRDD.window(Durations.seconds(60),Durations.seconds(15));
        windowRDD.print();

        jssc.start();
        jssc.awaitTermination();

    }

    public static List<String> getUserRecentlyRating(int numMax, int uid, Jedis jedis) {
        List<String> values = jedis.lrange(String.valueOf(uid), 0L, numMax-1);

        return values;
    }


    public static void getTopSimMovies(int maxNum, int uid, int mid, Map simMovieMap) {
        List<String> simMovies = (List<String>)simMovieMap.get(mid);


    }

    public static void computeMovieScores(List<String> candidateMovies, List<String> userRecentRatings, Map simMovieMap) {

        // 定义一个HashMap，保存每一个备选电影的增强减弱因子
        Map increMap = new HashMap();
        Map decreMap = new HashMap();

        for(String eachMovie: candidateMovies; eachRating: userRecentRatings) {


        }

    }

    public static double getMoviesSimScore(int mid1, int mid2, Map simMovieMap) {

        //return simMovieMap.get(mid1);
        return 0.0;

    }
}
