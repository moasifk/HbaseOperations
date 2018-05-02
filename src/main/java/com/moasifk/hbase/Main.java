package com.moasifk.hbase;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Main {

	public static void main(String[] args) {
		HbaseOperations ops = new HbaseOperations();
		SparkConf conf = new SparkConf().setAppName("SparkStreamingHbaseExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		ops.createHbaseTable("bluevigil", "address");
		ops.insertDataIntoHbaseTable("bluevigil");
	}
}
