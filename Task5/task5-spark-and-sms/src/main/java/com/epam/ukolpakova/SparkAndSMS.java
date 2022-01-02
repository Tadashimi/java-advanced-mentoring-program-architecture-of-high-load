package com.epam.ukolpakova;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * Find top 5 spam words are not contained in ham messages (use smsData.txt).
 * Implement your solution via one of approaches (RDD/DataFrames/SparkSQL). Java or Scala as you wish.
 */

public class SparkAndSMS {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().text("src/main/resources/smsData.txt");
        List<String> hamMessages = df.filter(col("value").contains("ham")).as(Encoders.STRING()).collectAsList();
        Map<String, Long> hamWordsCount = getWordsCount(hamMessages);

        List<String> spamMessages = df.filter(col("value").contains("spam")).as(Encoders.STRING()).collectAsList();
        Map<String, Long> spamWordsCount = getWordsCount(spamMessages);

        spamWordsCount.values().removeIf(hamWordsCount::containsValue);
        spamWordsCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(5)
                .forEach(entry -> System.out.println("Word " + entry.getKey() + " appears " + entry.getValue() + " times"));

    }

    private static Map<String, Long> getWordsCount(List<String> strings) {
        return strings.stream()
                .map(s -> s.replaceAll("[\\W]", " "))
                .flatMap(Pattern.compile("\\s+")::splitAsStream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}
