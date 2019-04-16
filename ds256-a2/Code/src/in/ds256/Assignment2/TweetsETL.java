package in.ds256.Assignment2;

import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
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
import org.json.simple.parser.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;


public class TweetsETL {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n" + "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}

		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		
		SparkConf sparkConf = new SparkConf().setAppName("TweetETL");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));
		
		
		/**
		 * Code goes here....
		 */

		JavaDStream<String> input_lines = messages.map(ConsumerRecord::value).map(json -> {
			
			try {
				JSONParser jsonParser = new JSONParser();
				String epoch_time,timezone,lang,tweet_id,user_id,followers_count,friends_count,normalised_tweet,hashtags;
				JSONObject js = null;
				
					js = (JSONObject) jsonParser.parse(json);
				
				
				JSONObject user;
				user = (JSONObject)js.get("user");
				epoch_time=(String)js.get("created_at");
				
					lang=(String)js.get("lang");
			 
				
				
					user_id = (String)user.get("id_str");
									timezone=(String)user.get("time_zone");
					tweet_id=(String)js.get("id_str");
									followers_count=(String)user.get("followers_count");
					friends_count=(String)user.get("friends_count");
					normalised_tweet=(String)js.get("text");
					
					String stop_words="\r|\n|\\b(a|about|above|after|again|against|all|am|an|and|any|are|aren't|as|at|be|because|been|before|being"
						+ "|below|between|both|but|by|can't|cannot|could|couldn't|did|didn't|do|does|doesn't|doing|don't|down|during|"
							+ "each|few|for|from|further|had|hadn't|has|hasn't|have|haven't|having|he|he'd|he'll|he's|her|here|here's|hers|"
							+ "herself|him|himself|his|how|how's|i|i'd|i'll|i'm|i've|if|in|into|is|isn't|it|it's|its|itself|let's|me|more|most|"
							+ "mustn't|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|same|shan't|she|"
							+ "she'd|she'll|she's|should|shouldn't|so|some|such|than|that|that's|the|their|theirs|them|themselves|then|there|there's|"
							+ "these|they|they'd|they'll|they're|they've|this|those|through|to|too|under|until|up|very|was|wasn't|we|we'd|we'll|we're|"
							+ "we've|were|weren't|what|what's|when|when's|where|where's|which|while|who|who's|whom|why|why's|with|won't|would|wouldn't|you|"
							+ "you'd|you'll|you're|you've|your|yours|yourself|yourselves)\\b\\s?";
					
					normalised_tweet=normalised_tweet.replaceAll(stop_words, "");
					normalised_tweet = normalised_tweet.replaceAll("[^a-zA-Z ]","").toLowerCase();
					normalised_tweet=normalised_tweet.replaceAll("\\W", "");
		
				JSONObject entities;
				try {
					entities = (JSONObject)js.get("entities");
				} catch (Exception e1) {
					
					entities=null;
				}
				hashtags="";
				JSONArray jsonArray = null;
				try {
					jsonArray = (JSONArray) entities.get("hashtags");
					for(int i = 0; i < jsonArray.size(); i++)
					{
					      JSONObject objects = (JSONObject)jsonArray.get(i);
					      hashtags=hashtags+";"+((String)objects.get("text"));
					}
					
				} catch (Exception e) {
					
				}
				
				

				return epoch_time+" , "+timezone+" , "+lang+" , "+tweet_id+" , "+user_id+" , "+followers_count+" , "+friends_count+" , "+normalised_tweet+" , "+hashtags;
			} catch (Exception e) {
		
				return "cache block";
			}
			
		});
		
		input_lines.print();
		

	
		input_lines.dstream().saveAsTextFiles(output,"");
		
		
		jssc.start();
		jssc.awaitTermination();
	}
}
