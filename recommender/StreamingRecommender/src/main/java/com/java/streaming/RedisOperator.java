package com.java.streaming;

import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Random;

public class RedisOperator {

    public static void main(String[] args) throws Exception {
        System.out.println("This is the start of Redis Operator");


       Jedis jedis = new Jedis("10.1.1.100", 6379);

       jedis.set("key1", "values1");
       jedis.set("key2", "values2");

        setJedisValues(jedis, 200);
       System.out.println("get values " + jedis.get("key1") + " " + jedis.get("key2"));
    }

    public static void setJedisValues(Jedis jedis, int times) {
        String[] uids = {"2379" ,"1384", "2905", "2125", "1152", "2852"};
        String[] mids = {"26371" ,"30803", "2643", "2739", "1923", "2051"};


        for (int i=0; i< times; i++) {
            Date now = new Date();
            Random random = new Random();

            int rndDiff = random.nextInt(10);

            Date randomDate = new Date(now.getTime() - rndDiff * 1000 * 24 * 3600);

            int rndProvIndex = random.nextInt(5);

            StringBuffer valueBuf = new StringBuffer();
            valueBuf.append(mids[rndProvIndex]);
            valueBuf.append(":");

            valueBuf.append((double) rndProvIndex / 5);

            jedis.rpush(uids[rndProvIndex], valueBuf.toString());
        }
    }
}
