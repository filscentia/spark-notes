package com.filscentia.sparknotes;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class LoadDataApp {
    private LoadDataApp() {
    }

    /**
     * Display comprehensive information about a table
     */
    private static void displayTableInfo(SparkSession spark, String tableName) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println("TABLE: " + tableName);
        System.out.println("============================================================");

        try {
            // Read the table
            Dataset<Row> df = spark.table(tableName);

            // Display schema
            System.out.println();
            System.out.println("Schema:");
            df.printSchema();

            // Display row count
            long rowCount = df.count();
            System.out.println();
            System.out.println("Total Records: " + String.format("%,d", rowCount));

            // Display sample data
            System.out.println();
            System.out.println("Sample Data (first 10 rows) using dataframe api:");
            df.show(10, false);

            System.out.println();
            System.out.println("Sample Data (first 10 rows) using sql:");
            spark.sql("select * from " + tableName + " limit 10").show();

        } catch (Exception e) {
            System.out.println();
            System.out.println("✗ Error reading table '" + tableName + "': " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        // Initialize Spark Session
        System.out.println();
        System.out.println("============================================================");
        System.out.println("DISPLAYING SPARK TABLE INFORMATION");
        System.out.println("============================================================");

        SparkSession spark = SparkSession.builder()
                .appName("TableInfoViewer")
                .config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .enableHiveSupport()
                .getOrCreate();

        // Silence Spark framework logs
        spark.sparkContext().setLogLevel("ERROR");

        try {

            final Class clazz = Class.forName("org.apache.spark.sql.classic.Dataset");
            System.out.print(clazz);

            for (Method m : clazz.getMethods()) {
                if (m.getName().contains("ofRows")) {
                    System.out.println(m);
                }

            }

            // List available tables
            System.out.println();
            System.out.println("Available tables:");
            List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
            if (!tables.isEmpty()) {
                for (Row table : tables) {
                    System.out.println("  - " + table.getString(1));
                }
            } else {
                System.out.println("  No tables found");
            }

            // Display information for tests table
            displayTableInfo(spark, "tests");

            // Display information for vehicles table
            displayTableInfo(spark, "vehicles");

        } catch (Exception e) {
            System.out.println();
            System.out.println("✗ Error: " + e.getMessage());
        } finally {
            // Clean up
            spark.stop();
            System.out.println();
            System.out.println("Spark session stopped.");
            System.out.println();
        }
    }
}