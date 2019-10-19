package com.java.content;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import java.io.File;
import java.lang.Math;
import scala.Tuple2;
import java.util.Collections;
import java.util.Comparator;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.commons.collections.IteratorUtils;

import javax.security.sasl.SaslServer;


public class ItemFilterTest {

    static int max_prefs_per_user = 20;
    static int topn = 5;

    public static void main(String[] args) {
        System.out.println("This is the start of program");

        String inputPath = "/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/ratings.csv";

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");

        SparkConf sparkConf = new SparkConf().setMaster((String) configMap.get("spark.cores")).setAppName("OfflineRecommender");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Tuple2<String, Double>> inputDataRDD = context.textFile(inputPath).mapToPair(line -> {
            String[] lineItems = line.split(",");
            if (lineItems.length >= 3) {
                return new Tuple2<String, Tuple2<String, Double>>(lineItems[0],
                        new Tuple2<String, Double>(lineItems[1], Double.parseDouble(lineItems[2])));
            } else {
                return null;
            }
        }).filter(eachTuple2 -> {
            return eachTuple2 != null;
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groupedByUser = inputDataRDD.groupByKey();
        JavaPairRDD<String, Tuple2<String, Double>> listedByItem = groupedByUser.flatMapToPair(eachOne -> {
            String userId = eachOne._1;
            Iterator<Tuple2<String, Double>> iterator = eachOne._2.iterator();

            int i = 0;
            List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<Tuple2<String, Tuple2<String, Double>>>();
            while (iterator.hasNext() && i < max_prefs_per_user) {
                Tuple2<String, Double> tupleEach = (Tuple2<String, Double>) iterator.next();
                list.add(new Tuple2<String, Tuple2<String, Double>>(tupleEach._1, new Tuple2<String, Double>(userId, tupleEach._2)));
                i++;
            }
            return list.iterator();
        });

        File filea = new File("item_user_score.csv");
        listedByItem.foreach(eachStr -> {
            FileUtils.writeStringToFile(filea, eachStr._1 + " " + eachStr._2._1 + " " + eachStr._2._2 + "\r\n", true);
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groupedByItem = listedByItem.groupByKey();
        JavaPairRDD<String, Tuple2<String, Double>> listedByUserNormalized = groupedByItem.flatMapToPair(eachOne -> {
            String itemId = eachOne._1;
            Iterator<Tuple2<String, Double>> iterator = eachOne._2.iterator();

            Object[] itemScoreArray = IteratorUtils.toArray(iterator);
            double sum = 0.0;
            for (int i = 0; i < itemScoreArray.length; i++) {
                Tuple2<String, Double> tuple2 = (Tuple2<String, Double>) itemScoreArray[i];  //iterator.next();  //
                sum += Math.pow(tuple2._2, 2);
            }
            sum = Math.sqrt(sum);

            List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<Tuple2<String, Tuple2<String, Double>>>();
            for (int j = 0; j < itemScoreArray.length; j++) {
                Tuple2<String, Double> tuple2 = (Tuple2<String, Double>) itemScoreArray[j];
                ;
                double score = tuple2._2 / sum;
                list.add(new Tuple2<String, Tuple2<String, Double>>(tuple2._1, new Tuple2<String, Double>(itemId, score)));
            }
            return list.iterator();
        });
        File file1 = new File("user_item_score_normalized.csv");
        listedByUserNormalized.foreach(eachStr -> {
            FileUtils.writeStringToFile(file1, eachStr._1 + " " + eachStr._2._1 + " " + eachStr._2._2 + "\r\n", true);
        });
       JavaPairRDD<String,Iterable<Tuple2<String, Double>>> userItemNormalized = listedByUserNormalized.groupByKey();

        JavaPairRDD<Tuple2<String, String>,Iterable<Double>> scoreMultiply = userItemNormalized.flatMapToPair(eachRow->{
            Iterable<Tuple2<String, Double>> iterable = eachRow._2;
            Object[] itemScoreArray = IteratorUtils.toArray(iterable.iterator());

            List<Tuple2<Tuple2<String,String>, Double>> list = new ArrayList<Tuple2<Tuple2<String,String>, Double>>();
            for(int i=0; i<itemScoreArray.length; i++) {
                for (int j=i+1; j<itemScoreArray.length ; j++) {
                    String itema = ((Tuple2<String, Double>)itemScoreArray[i])._1;
                    String itemb = ((Tuple2<String, Double>)itemScoreArray[j])._1;

                    double scorea = ((Tuple2<String, Double>)itemScoreArray[i])._2;
                    double scoreb = ((Tuple2<String, Double>)itemScoreArray[j])._2;

                    list.add(new Tuple2<Tuple2<String,String>, Double>(new Tuple2<String,String>(itema, itemb), scorea * scoreb));
                    list.add(new Tuple2<Tuple2<String,String>, Double>(new Tuple2<String,String>(itemb, itema), scorea * scoreb));
                }
            }

            return list.iterator();
        }).groupByKey();

        JavaPairRDD<String,Iterable<Tuple2<String, Double>>>  scoreGroupBy= scoreMultiply.mapToPair(row -> {
            Tuple2<String,String> itemPair = row._1;
            Object[] scoreMulti = IteratorUtils.toArray(row._2.iterator());
            double score = 0.0;

            for (int i=0; i<scoreMulti.length; i++) {
                score+=(Double)scoreMulti[i];
            }
            return new Tuple2<String, Tuple2<String, Double>>(itemPair._1, new Tuple2<String, Double>(itemPair._2, score));
        }).groupByKey();

        JavaRDD<String> outputRDD = scoreGroupBy.map(eachOne -> {
            String itema = eachOne._1;
            List<Tuple2<String, Double>> itemsScore = IteratorUtils.toList(eachOne._2.iterator());
            Collections.sort( itemsScore, new Comparator<Tuple2<String, Double>>() {
                @Override
                public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                    return (int)(o2._2 - o1._2);
                }
            });

            int itemsSize = itemsScore.size();
            if (itemsSize > topn) {
                itemsSize = topn;
            }
            StringBuilder outputStr = new StringBuilder();
            Iterator<Tuple2<String, Double>> iterator = itemsScore.iterator();
            int i = 0;
            while(iterator.hasNext() && i<itemsSize) {
                Tuple2<String, Double> eachScore = iterator.next();
                String item = eachScore._1;
                outputStr.append(item).append(" ").append(eachScore._2).append(";");
                i++;
            }

            return itema + " " + outputStr.toString() + "\r\n";
        });

        outputRDD.foreach(eachStr -> {
            FileUtils.writeStringToFile(new File("output.csv"), eachStr + "\r\n", true);
        });

        // System.out.println("count " + userItemNormalized.count());
    }
}
