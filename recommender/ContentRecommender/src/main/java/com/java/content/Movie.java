package com.java.content;

public class Movie {
    //class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
    //shoot: String, language: String, genres: String, actors: String, directors: String)

    int mid;
    String name;
    String descri;
    String timelong;

    String issue;
    String shoot;

    String language;
    String genres;

    String actors;
    String directors;


    public Movie(int mid, String name, String descri, String timelong, String issue, String shoot, String language, String genres, String actors, String directors) {
        this.mid = mid;
        this.name = name;
        this.descri = descri;
        this.timelong = timelong;
        this.issue = issue;
        this.shoot = shoot;
        this.language = language;
        this.genres = genres;
        this.actors = actors;
        this.directors = directors;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescri() {
        return descri;
    }

    public void setDescri(String descri) {
        this.descri = descri;
    }

    public String getTimelong() {
        return timelong;
    }

    public void setTimelong(String timelong) {
        this.timelong = timelong;
    }

    public String getIssue() {
        return issue;
    }

    public void setIssue(String issue) {
        this.issue = issue;
    }

    public String getShoot() {
        return shoot;
    }

    public void setShoot(String shoot) {
        this.shoot = shoot;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public String getActors() {
        return actors;
    }

    public void setActors(String actors) {
        this.actors = actors;
    }

    public String getDirectors() {
        return directors;
    }

    public void setDirectors(String directors) {
        this.directors = directors;
    }
}
