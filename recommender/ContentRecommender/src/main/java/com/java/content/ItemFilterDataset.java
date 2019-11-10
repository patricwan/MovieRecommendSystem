package com.java.content;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.rank;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ItemFilterDataset {

    public static void main(String[] args) {
        System.out.println("This is the start of program Item Filter Dataset");

        Map configMap = new HashMap();
        configMap.put("spark.cores", "local[*]");
        String csvPath = "/root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/ratings.csv";

        SparkConf sparkConf = new SparkConf().setMaster((String) configMap.get("spark.cores")).setAppName("ItemFilterDataset");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> originalDataset = sparkSession.read().csv(csvPath);

        //originalDataset.show();

        StructType structType = new StructType();
        structType = structType.add("user", DataTypes.StringType, true);
        structType = structType.add("item", DataTypes.StringType, true);
        structType = structType.add("score", DataTypes.DoubleType, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> mappedData = originalDataset.map(originalRow-> {
            return RowFactory.create(originalRow.get(0), originalRow.get(1), Double.parseDouble((String)originalRow.get(2)));
        }, encoder)  ;
        mappedData.show();
        mappedData.createOrReplaceTempView("useritemscore");

        Dataset<Row> groupedByItem = sparkSession.sql(
                "select item as item, sum(score * score) as sumSquare from useritemscore group by item");
        groupedByItem.show();

        Dataset<Row> joinedDataset = mappedData.join(groupedByItem, mappedData.col("item").equalTo(groupedByItem.col("item")), "left")
                .withColumn("normalizedScore", col("score").divide(sqrt(col("sumSquare"))));
        joinedDataset.show();



        sparkSession.close();
    }
}