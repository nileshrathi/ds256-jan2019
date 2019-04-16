package in.ds256.Assignment2;

import java.util.HashMap;
import java.util.Locale;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;



public class UserCount {

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
		SparkConf sparkConf = new SparkConf().setAppName("UserCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/**
		*	Code goes here....
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
					String created_at=epoch_time;
		
									DateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
								    DateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
								    inputFormat.setLenient(true);

								    Date date = inputFormat.parse(created_at);
								    
								    Calendar cal = Calendar.getInstance();
								    cal.setTime(date);
								    Integer hours = cal.get(Calendar.HOUR_OF_DAY);
								    
								    Integer year= cal.get(Calendar.YEAR);
								    Integer month= cal.get(Calendar.MONTH);
								    Integer day= cal.get(Calendar.DATE);
								    
								    String time= year.toString() + "-" + month.toString() + "-" + day.toString() + "-" + hours.toString();
				
				

				return time+"----"+user_id;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				return "cache block";
			}
			
		}).transform(x->x.distinct());
		/*
		csv_row.foreachRDD( row-> {
	        row.collect().stream().forEach(n-> System.out.println(n));
	    });
	    */
JavaPairDStream<String, String> timeToUser=input_lines.mapToPair(new PairFunction<String, String, String>() {
    public Tuple2<String, String> call(String arg0) throws Exception {
    	String[] split=arg0.split("----");
    	String time=split[0];
    	String userid=split[1];
        return new Tuple2<String,String> (time,userid);
    }
});
JavaPairDStream<String, Iterable<String>> timetoUniqueUserGroup = timeToUser.groupByKey();


     
timetoUniqueUserGroup.print();
		

		/*
		csv_row.foreachRDD(str -> {
			str.saveAsTextFile(output);
		});
		*/
timetoUniqueUserGroup.dstream().saveAsTextFiles(output,"");
		
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}


