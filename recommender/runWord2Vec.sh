rm -rf /root/github/MovieRecommendSystem/recommender/KMeansModel
rm -rf /root/github/MovieRecommendSystem/recommender/ContentRecommender/src/main/resources/wordmodelarticle.txt*
java -cp ContentRecommender/target/ContentRecommender-1.0-SNAPSHOT-jar-with-dependencies.jar com.java.content.JavaWord2VecExample
