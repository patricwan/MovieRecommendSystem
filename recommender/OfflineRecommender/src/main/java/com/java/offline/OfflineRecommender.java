package com.java.offline;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

import java.util.HashMap;import org.apache.spark.ml.Model;
import java.util.*;
import org.jblas.DoubleMatrix;
import org.jblas.FloatMatrix;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.Map;

//https://www.cnblogs.com/a-du/p/10947743.html
public class OfflineRecommender {

    static String  MONGODB_RATING_COLLECTION = "Rating";

    static String  USER_RECS = "UserRecs";
    static String  MOVIE_RECS = "MovieRecs";

    static int USER_MAX_RECOMMENDATION = 20;

    public static void main(String[] args) {
        System.out.println("This is the Offline Recommender Start");

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        configMap.put("mongo.uri", "mongodb://10.1.1.100:27017/recommender");
        configMap.put("mongo.db", "recommender");

        SparkConf sparkConf = new SparkConf().setMaster((String)configMap.get("spark.cores")).setAppName("OfflineRecommender");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> datasetRating = spark.read().option("uri", (String)configMap.get("mongo.uri")).option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load().select("_id.oid", "mid", "score");;
        datasetRating.show();

        Dataset<Row> usersDistinct = datasetRating.select(datasetRating.col("oid")).distinct();
        usersDistinct.show();

        Dataset<Row> moviesDistinct = datasetRating.select(datasetRating.col("mid")).distinct();
        moviesDistinct.show();

        JavaPairRDD<Row,Row> result = usersDistinct.toJavaRDD().cartesian(moviesDistinct.toJavaRDD());

        //Model model = ALS.train(datasetRating, 200, 5, 1);
        datasetRating.printSchema();
        Dataset<Tuple3<Integer, Integer, Double>> datasetRatingALS = datasetRating.map(line -> {
            //System.out.println("first " + line.get(0));
            int userId = convertStrToUniqueInt((String)line.get(0));
            Tuple3<Integer, Integer, Double> tuple = new Tuple3<Integer, Integer, Double>(
                    userId, line.getInt(1), line.getDouble(2)
            );
            return tuple;
        }, Encoders.tuple(Encoders.INT(), Encoders.INT(), Encoders.DOUBLE()));

        datasetRatingALS.show();

        Dataset<Row> allData = datasetRatingALS.toDF("user", "movie", "rating").cache();
        double[] splitData = new double[]{0.8,0.2};
        Dataset<Row>[] splits = allData.randomSplit(splitData);

        Dataset<Row> trainData = splits[0];

        long rndLong = (long) Math.random() * 99999;
        ALSModel alsModel = new ALS().setSeed(rndLong).setImplicitPrefs(true).setRank(10).setRegParam(0.01)
                .setAlpha(1.0).setMaxIter(5).setUserCol("user").setItemCol("movie").setRatingCol("rating")
                .setPredictionCol("prediction").fit(trainData);
        trainData.unpersist();

        alsModel.userFactors().show();

        alsModel.itemFactors().show();


        Dataset<Row> toRecommend = null;
        Dataset<Row>  predictions = alsModel.transform(splits[1]);
        predictions.show();

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("mse")//RMSE Error
                .setLabelCol("rating")
                .setPredictionCol("prediction");

        Double rmse = evaluator.evaluate(predictions);
        System.out.println("rmse " + rmse);

        Dataset<Row> featuresData = alsModel.itemFactors().select("id", "features");
        featuresData.printSchema();

        Encoder<Tuple2<Integer, float[]>> tupleEncoder = Encoders.tuple(Encoders.INT(),
                spark.implicits().newFloatArrayEncoder());
        Dataset<Tuple2<Integer, float[]>> tupleDataFeature = featuresData.map(row -> {
            WrappedArray<Float> iterable = row.getAs(1);
            List<Float> doublelist = new ArrayList<Float>();

            JavaConversions.asJavaCollection(iterable).forEach(t-> {
                //System.out.println("t "+ t);
                doublelist.add((Float)t);
            });

            int i = 0;
            float[] doubleData = new float[doublelist.size()];
            for (Float eachF : doublelist) {
                doubleData[i] = eachF.floatValue();
                //System.out.println(" value " + eachF + " i " + i);
                i++;
            }

            //double[] arrayFloat =  vector.toArray();
            return new Tuple2<Integer, float[]>(row.getInt(0), doubleData);
        }, tupleEncoder);

        JavaRDD<Tuple2<Integer, float[]>> itemFeaturesRdd = tupleDataFeature.rdd().toJavaRDD();
        JavaPairRDD<Integer, FloatMatrix> javaPairMatrix =  itemFeaturesRdd.mapToPair(new PairFunction<Tuple2<Integer, float[]>, Integer, FloatMatrix>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, FloatMatrix> call(Tuple2<Integer, float[]> tuple) throws Exception {
                return new Tuple2(tuple._1, new FloatMatrix(tuple._2));
            }
        });

        JavaPairRDD<Tuple2<Integer, FloatMatrix>, Tuple2<Integer, FloatMatrix>> cartesianRDD = javaPairMatrix.cartesian(javaPairMatrix)
                .filter( new Function<Tuple2<Tuple2<Integer, FloatMatrix>, Tuple2<Integer, FloatMatrix>>, Boolean>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, FloatMatrix>, Tuple2<Integer, FloatMatrix>> tuple) throws Exception {
                return tuple._1._1 != tuple._2._1;
            }
        });

        JavaPairRDD<Integer, Tuple2<Integer, Float>> scoreRDD = cartesianRDD.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, FloatMatrix>, Tuple2<Integer, FloatMatrix>>, Integer, Tuple2<Integer, Float>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, Tuple2<Integer, Float>> call(Tuple2<Tuple2<Integer, FloatMatrix>, Tuple2<Integer, FloatMatrix>> tuple) throws Exception {
                float simScore = consinSim(tuple._1._2, tuple._2._2);
                return new Tuple2<Integer, Tuple2<Integer, Float>>(tuple._1._1, new Tuple2<Integer, Float>(tuple._2._1, simScore));
            }
        }).filter(tuple -> {
            return tuple._2._2 > 0.6;
        });

        JavaPairRDD<Integer,Iterable<Tuple2<Integer, Float>>> groupedSim = scoreRDD.groupByKey();
        JavaPairRDD<Integer, String> recommendedPairRDD= groupedSim.mapToPair(new PairFunction<Tuple2<Integer,Iterable<Tuple2<Integer, Float>>>, Integer,String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer,String> call(Tuple2<Integer,Iterable<Tuple2<Integer, Float>>> tuple) throws Exception {
                Iterator recommendedIter = tuple._2.iterator();
                List list = new ArrayList<Tuple2<Integer, Float>>();
                StringBuilder strRecomm = new StringBuilder();
                while(((Iterator) recommendedIter).hasNext()) {
                    Tuple2<Integer, Float> nextOne = (Tuple2<Integer, Float>)recommendedIter.next();
                    list.add(nextOne);
                    strRecomm.append(nextOne._1);
                    strRecomm.append("_");
                }
                return new Tuple2<Integer,String>(tuple._1, strRecomm.toString());
            }
        });

        recommendedPairRDD.foreach(row -> {
            System.out.println(row);
        });

    }

    public static int convertStrToUniqueInt(String str) {
        int result = 0;
        StringBuilder strValue = new StringBuilder();
        for (int i = 0; i < str.length(); i=i+2) {
            int value = str.charAt(i)-'0';
            strValue.append(value+"");
        }


        return (int)(Long.parseLong(strValue.toString()) % (Integer.MAX_VALUE - 1));
    }
    public static float consinSim(FloatMatrix item1, FloatMatrix item2) {
        return item1.dot(item2) / ( item1.norm2() * item2.norm2() );
    }

}