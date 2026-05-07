package com.filscentia.sparknotes;

import java.util.Base64;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.SparkSession;

import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.spark.logical.ToLogicalPlan;

@SuppressWarnings("PMD")
public final class HelloSpark {

    private HelloSpark() {
    }

    public static void main(final String[] args) {
    
        final String PLAN_B64="Gt8DEtwDCuMCGuACCgIKABLVAjrSAgoNEgsKCQkKCwwNDg8QERLWAQrTAQoCCgASwgEKCk9fT1JERVJLRVkKCU9fQ1VTVEtFWQoNT19PUkRFUlNUQVRVUwoMT19UT1RBTFBSSUNFCgtPX09SREVSREFURQoPT19PUkRFUlBSSU9SSVRZCgdPX0NMRVJLCg5PX1NISVBQUklPUklUWQoJT19DT01NRU5UEkoKBDoCEAIKBDoCEAIKB6oBBAgBGAIKCcIBBggCEA8gAgoFggECEAIKB6oBBAgPGAIKB6oBBAgPGAIKBCoCEAIKB7IBBAhPGAIYAjoICgZPUkRFUlMaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGAAgChIKT19PUkRFUktFWRIJT19DVVNUS0VZEg1PX09SREVSU1RBVFVTEgxPX1RPVEFMUFJJQ0USC09fT1JERVJEQVRFEg9PX09SREVSUFJJT1JJVFkSB09fQ0xFUksSDk9fU0hJUFBSSU9SSVRZEglPX0NPTU1FTlQyCxBVKgdpc3RobXVz";


        System.out.println();
        System.out.println("============================================================");
        System.out.println("Hello from Spark with Hive Metastore");
        System.out.println("============================================================");

        org.apache.spark.sql.SparkSessionBuilder sb = org.apache.spark.sql.classic.SparkSession.builder();
        sb.appName(PLAN_B64).master("local").appName("hellospark");
        

        SparkSession spark = sb.getOrCreate();

        // Silence Spark framework logs
        spark.sparkContext().setLogLevel("ERROR");
        
        try {
            
            // Load orders parquet file into a table
            String ordersPath = "../containers/_data/orders.parquet";
            spark.read()
                .parquet(ordersPath)
                .createOrReplaceTempView("orders");
            
            System.out.println("\nFirst 10 rows from orders table:");
            System.out.println("============================================================");
            spark.sql("SELECT * FROM orders LIMIT 10").show();
            

            // Decode base64 Substrait plan
            System.out.println("\nExecuting Substrait Plan:");
            System.out.println("============================================================");
            
            byte[] planBytes = Base64.getDecoder().decode(PLAN_B64);
            io.substrait.proto.Plan protoPlan = io.substrait.proto.Plan.parseFrom(planBytes);
            
            // Convert proto plan to Substrait plan
            ProtoPlanConverter protoToPlan = new ProtoPlanConverter();
            Plan substraitPlan = protoToPlan.from(protoPlan);
            
            // Convert Substrait plan to Spark logical plan
            ToLogicalPlan substraitConverter = new ToLogicalPlan(spark);
            LogicalPlan sparkPlan = substraitConverter.convert(substraitPlan);
            
        
            // Execute the plan and show results using classic API
            Dataset.ofRows((org.apache.spark.sql.classic.SparkSession)  spark, sparkPlan).show();

        } catch (Throwable t) {
            System.out.println();
            System.out.println("✗ Error: " + t.getMessage());
            t.printStackTrace();
        } finally {
            spark.stop();
            System.out.println();
            System.out.println("Spark session stopped.");
            System.out.println();
        }
    }}