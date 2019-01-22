import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.bson.Document;
import scala.Tuple2;

import java.util.regex.*;

public class SimpleConsumer {
   private static final Pattern SPACE = Pattern.compile(" ");
   public static void main(String[] args) throws Exception {

      //Kafka consumer configuration settings
      String topicName = "Kafka";
      Map<String, Object> props = new HashMap();

      SparkSession sparkSession = SparkSession.builder()
              .appName("JavaDirectKafkaWordCount")
              .master("local[1]")
              .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test")
              .config("spark.mongodb.output.collection", "spark")
              .getOrCreate();
      JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
      JavaStreamingContext jssc = new JavaStreamingContext(jsc, Duration.apply(2));

//      Map<String, String> writeOverrides = new HashMap<>();
////      writeOverrides.put("spark.mongodb.output.collection", "spark");
//      writeOverrides.put("writeConcern.w", "majority");
//      WriteConfig writeConfig = WriteConfig.create(jssc.sparkContext()).withOptions(writeOverrides);

      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);

      Set<String> topicsSet = new HashSet<>(Arrays.asList(topicName));

      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);
//      int i = 0;
//
//      while (true) {
//         ConsumerRecords<String, String> records = consumer.poll(100);
//         for (ConsumerRecord<String, String> record : records)
//
//         // print the offset,key and value for the consumer records.
//         System.out.printf("offset = %d, key = %s, value = %s\n",
//            record.offset(), record.key(), record.value());
//      }

      // Create direct kafka stream with brokers and topics
      JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
              jssc,
              LocationStrategies.PreferConsistent(),
              ConsumerStrategies.Subscribe(topicsSet, props));

      // Get the lines, split them into words, count the words and print
      JavaPairDStream<String, Integer> wordCounts = messages
              .map(ConsumerRecord::value)
              .flatMap(x -> Arrays.asList(SPACE.split(x)).iterator())
              .mapToPair(s -> new Tuple2<>(s, 1))
              .reduceByKey((i1, i2) -> i1 + i2);
//              .map(WordCounter::new);
       ObjectMapper mapper = new ObjectMapper();
      wordCounts.print();
      wordCounts
              .map(WordCounter::new)
              .map(wc -> Document.parse(mapper.writeValueAsString(wc)))
              .foreachRDD(rdd -> {
          System.out.println("Saving to mong");
          MongoSpark.load(jsc).filter(d -> d.getString("word").equals());
          MongoSpark.save(rdd);
      });
      // Start the computation
      jssc.start();
      jssc.awaitTermination();
   }
}