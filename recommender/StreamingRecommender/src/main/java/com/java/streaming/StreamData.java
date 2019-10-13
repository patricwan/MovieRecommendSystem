package com.java.streaming;

public class StreamData {

    public StreamData() {

    }

    public StreamData(int uid, int mid, double score, long logTime) {
        this.uid = uid;
        this.mid = mid;
        this.score = score;
        this.logTime = logTime;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public long getLogTime() {
        return logTime;
    }

    public void setLogTime(long logTime) {
        this.logTime = logTime;
    }

    private int uid;

    private int mid;

    private double score;

    private long logTime;



}
