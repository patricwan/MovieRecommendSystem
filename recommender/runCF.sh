rm -rf /root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/cfoutput.csv
java -cp ContentRecommender/target/ContentRecommender-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.content.cf
java -cp ContentRecommender/target/ContentRecommender-1.0-SNAPSHOT-jar-with-dependencies.jar com.java.content.ItemFilterTest
