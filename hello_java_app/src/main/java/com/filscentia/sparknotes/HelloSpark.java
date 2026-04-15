package com.filscentia.sparknotes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public final class HelloSpark {

    private HelloSpark() {
    }

    public static void main(final String[] args) {
 
        System.out.println();
        System.out.println("============================================================");
        System.out.println("Hello from Spark");
        System.out.println("============================================================");

 
        final SparkConf conf = new SparkConf()
                .setAppName("HelloWorldSpark");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            final long count = sc
                    .parallelize(java.util.List.of("Hello", "World"))
                    .count();

            System.out.println("Hello from Apache Spark!");
            System.out.println("Element count: " + count);
        }

    }
}