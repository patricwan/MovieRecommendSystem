package com.java.streaming;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;


public class KafkaTestProducer {

    public static void main(String[] args) throws Exception {
        System.out.println("This is the start of Kafka producer messages repetitively.");


        Producer producer = createNewProducerDefault();
        System.out.println("Producer created " + producer.toString());
        produce(producer, "recommender",50000);

    }

    private static Producer createNewProducerDefault() throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "10.1.1.95:9092");

        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", "0");
        kafkaProps.put("linger.ms", "1");

        kafkaProps.put("linger.ms", "1");
        //kafkaProps.put("partitioner.class", "com.sap.hadoop.test.kafka.SimplePartition");

        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String, String>(kafkaProps);
        return producer;
    }

    private static void produce(Producer producer, String topic, int count) throws Exception {
        System.out.println("Start to send topics");
        for (int i = 1; i <= count; i++) {
            String value = generateRandomLog();
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, String.valueOf(i), value);
            System.out.println("Start to send msg " + msg);
            producer.send(msg);
            System.out.println("Sent msg " + msg);
            Thread.sleep(2000);
        }
    }

    private static String generateRandomLog() {

        //UID|MID|SCORE|TIMESTAMP

        String[] uids = {"2379" ,"1384", "2905", "2125", "1152", "2852"};
        String[] mids = {"26371" ,"30803", "2643", "2739", "1923", "2051"};

        StringBuffer logBuf = new StringBuffer();
        Random random = new Random();

        Date now = new Date();
        int rndDiff = random.nextInt(10);
        Date randomDate = new Date(now.getTime() - rndDiff * 1000 * 24 * 3600);

        int rndProvIndex = random.nextInt(5);

        logBuf.append(uids[rndProvIndex]);
        logBuf.append("|");

        logBuf.append(mids[rndProvIndex]);
        logBuf.append("|");

        logBuf.append((double)rndProvIndex/5);
        logBuf.append("|");

        logBuf.append(randomDate.getTime());

        return logBuf.toString();
    }
}
