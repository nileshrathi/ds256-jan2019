package in.ds256.Assignment2;

import java.util.HashMap;
import java.util.Locale;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

public class HashTagTrends {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n"
					+ "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}
	
		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		// Create context with a 10 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("HashTagTrends");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/**
		*	Code goes here....
		*/
		
		
		JavaPairDStream<String,Long> json= messages.map(ConsumerRecord::value).flatMapToPair(f->{
			
			
			ArrayList<Tuple2<String,Long>> timed_hashtags=new ArrayList<Tuple2<String,Long>>();
			
			JSONParser jsonParser = new JSONParser();
			String epoch_time,timezone,lang,tweet_id,user_id,followers_count,friends_count,normalised_tweet,hashtags;
			JSONObject js = null;
			
				js = (JSONObject) jsonParser.parse(f);
				epoch_time=(String)js.get("created_at");
				
				DateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
			    DateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
			    inputFormat.setLenient(true);

			    Date date = inputFormat.parse(epoch_time);
			    
			    Calendar cal = Calendar.getInstance();
			    cal.setTime(date);
			    Integer hours = cal.get(Calendar.HOUR_OF_DAY);
			    Integer per_thirty_minutes=cal.get(Calendar.MINUTE)/30; 
			    Integer year= cal.get(Calendar.YEAR);
			    Integer month= cal.get(Calendar.MONTH);
			    Integer day= cal.get(Calendar.DATE);
			    Integer week= cal.get(Calendar.WEEK_OF_MONTH);
			    
			    
			    String time_hourly= year.toString() + "-" + month.toString() + "-" +week.toString()+"-"+day.toString() + "-" + hours.toString();
			    String time_daily= year.toString() + "-" + month.toString() + "-" +week.toString()+"-"+day.toString();
			    String time_weekly= year.toString() + "-" + month.toString() + "-" +week.toString();
				String time_minutes= year.toString() + "-" + month.toString() + "-" +week.toString()+"-"+day.toString() + "-" + hours.toString() + per_thirty_minutes.toString();
				
				
				JSONObject entities;
				try {
					entities = (JSONObject)js.get("entities");
				} catch (Exception e1) {
					
					entities=null;
				}
				
				JSONArray jsonArray = null;
				try {
					jsonArray = (JSONArray) entities.get("hashtags");
					for(int i = 0; i < jsonArray.size(); i++)
					{
					      JSONObject objects = (JSONObject)jsonArray.get(i);
					      String hashtag=((String)objects.get("text"));
					      timed_hashtags.add(new Tuple2<String,Long>(time_minutes+"-"+hashtag,1L));
					      	
					}
					
				} catch (Exception e) {
					
				}
			
			
			
			return timed_hashtags.iterator();
		}).reduceByKey((x,y)->x+y);
		
		JavaPairDStream<Long,String> swappedPair=json.mapToPair(Tuple2::swap);
		 JavaPairDStream<Long,String> sortedStream = swappedPair.transformToPair(s -> s.sortByKey(false));
		 JavaPairDStream<String, Long> original_format= sortedStream.mapToPair(Tuple2::swap);
		 
		 
		 original_format.print();
		 original_format.dstream().saveAsTextFiles(output, "");
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
