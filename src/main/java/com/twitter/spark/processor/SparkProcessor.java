package com.twitter.spark.processor;

import com.twitter.spark.processor.models.TweetRecord;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SparkProcessor {
    static DatumWriter <TweetRecord> userDatumWriter = new SpecificDatumWriter<> (TweetRecord.class);
    static DataFileWriter <TweetRecord> dataFileWriter = new DataFileWriter<> (userDatumWriter);

    public static void main(String[] args) throws InterruptedException, IOException {
        dataFileWriter.create(TweetRecord.getClassSchema(), new File("src/main/resources/tweets.avro"));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        kafkaParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "300000");

        System.out.println("Enter Topics to follow");
        Collection<String> topics = new ArrayList<>();
        Scanner in = new Scanner(System.in);
        topics.add( in .next());
        topics.add( in .next());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("MessageLoggingApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        streamingContext.sparkContext().setLogLevel("ERROR");
        JavaInputDStream <ConsumerRecord<String, String >> inputDStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shut Gracefully | Stopping Application\n");
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Data File Writer Closed | Done!");
        }));

        inputDStream.foreachRDD(rdd -> rdd.foreach(record -> {
                System.out.printf("topic = %s, partition = %d, offset = %d, key = %s",
                        record.topic(), record.partition(), record.offset(), record.key());
        System.out.println("\n\n" + record.value());

        JSONObject parentJsonObject = new JSONObject(record.value());
        JSONObject userJsonObject = parentJsonObject.getJSONObject("user");

        dataFileWriter.append(TweetRecord.newBuilder()
                .setTweetID(parentJsonObject.getString("id"))
                .setTweetDescription(parentJsonObject.getString("text").replace("\n", " ").replace("\t", "  "))
                .setCreationDtTm(parentJsonObject.getString("created_at"))
                .setTweetUserID(userJsonObject.getString("id"))
                .setTweetUserName(userJsonObject.getString("name"))
                .setTweetUserLocation(userJsonObject.getString("location"))
                .build());
        }));

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}