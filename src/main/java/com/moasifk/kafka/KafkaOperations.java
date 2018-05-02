package com.moasifk.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaOperations {
	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		// Itversity
		// props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667");
		// bluevigil
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "edge01.infrastruct.in:6667");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaSparkExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<String, String>(props);
	}
	
	public static void main(String args[]) {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingKafkaExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		String brokers = "edge01.infrastruct.in:6667";
//		String brokers = "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667";
		String zookeeper = "edge01.infrastruct.in:2181,master01.infrastruct.in:2181,master02.infrastruct.in:2181";
//		String zookeeper = "nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181";
		String topics = "http_src_topic";
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("zookeeper.connect", zookeeper);

		// Create direct kafka stream with brokers and topics
		System.out.println("************************KafkaUtils");
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// final Producer<String, String> producer = createProducer();
		System.out.println("************************Consuming kafka topic");
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> line) throws Exception {
				System.out.println("*line line line******: "+line._2());
				Producer<String, String> producer = createProducer();
            	ProducerRecord<String, String> record = new ProducerRecord<String, String>("BluevigilDev", "key",
            			line._2());
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("****************Send to Kafka**************");
				
				
				Configuration config = HBaseConfiguration.create();
//		        config.set("hbase.zookeeper.quorum", "nn01.itversity.com,nn02.itversity.com,rm01.itversity.com");
				config.set("hbase.zookeeper.quorum", "edge01.infrastruct.in,master01.infrastruct.in,master02.infrastruct.in");
		        config.set("hbase.zookeeper.property.clientPort", "2181");
		        config.set("zookeeper.znode.parent", "/hbase-unsecure");
		        Connection connection = null;
		        Table table = null;
		        try {
		            connection = ConnectionFactory.createConnection(config);
		            table = connection.getTable(TableName.valueOf("bluevigil"));
		            
		            Put employee1 = new Put(Bytes.toBytes("3"));
		            employee1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("name"), Bytes.toBytes("Mohammed Asif"));
		            employee1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("location"), Bytes.toBytes("India"));
		            table.put(employee1);
		            
		            Put employee2 = new Put(Bytes.toBytes("4"));
		            employee2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("name"), Bytes.toBytes("John"));
		            employee2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("location"), Bytes.toBytes("US"));
		            table.put(employee2);
		            
		        } catch (Exception e) {
		            e.printStackTrace();
		        } finally {
		            try {
		                if (table != null) {
		                	table.close();
		                }

		                if (connection != null && !connection.isClosed()) {
		                    connection.close();
		                }
		            } catch (Exception e2) {
		                e2.printStackTrace();
		            }
		        }
		        
				return line._2();
			}

		});
		
		/*lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
		    public void call(JavaRDD<String> rdd) throws Exception {
		        rdd.foreach(new VoidFunction<String>() {
		            public void call(String s) throws Exception {
		            	Producer<String, String> producer = createProducer();
		            	ProducerRecord<String, String> record = new ProducerRecord<String, String>("BluevigilDev", "key",
								s);
						RecordMetadata metadata = producer.send(record).get();
		            }
		        });
		    }
		});*/
		
		lines.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
