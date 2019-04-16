package in.ds256.Assignment2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import scala.Tuple2;

public class SentimentAnalysis {
	
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
		
		@SuppressWarnings("serial")
		JavaPairDStream<String,Long> sentiment_to_count= messages.map(ConsumerRecord::value).flatMapToPair(f->{
			
			ArrayList<Tuple2<String,Long>> sentiment_count_list=new ArrayList<Tuple2<String,Long> >();
			JSONParser jsonParser = new JSONParser();
			
			JSONObject js = null;
			try {
				js = (JSONObject) jsonParser.parse(f);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				return null;
			}
			
			String normalised_tweet,hashtags;
			
			JSONObject entities;
			try {
				entities = (JSONObject)js.get("entities");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				entities=null;
			}
			hashtags="";
			JSONArray jsonArray = null;
			try {
				jsonArray = (JSONArray) entities.get("hashtags");
				for(int i = 0; i < jsonArray.size(); i++)
				{
				      JSONObject objects = (JSONObject)jsonArray.get(i);
				      hashtags=hashtags+" "+((String)objects.get("text"));
				}
				
			} catch (Exception e) {
				
			}
			
			try {
				normalised_tweet=(String)js.get("text");
				normalised_tweet = normalised_tweet.replaceAll("[^a-zA-Z ]","").toLowerCase();
				normalised_tweet=normalised_tweet.replaceAll("\\W", "");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				normalised_tweet="";
			}
			
			String created_at="";
			
			
			try {
				created_at=(String)js.get("created_at");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				created_at="";
			}
			
			//DateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm");
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
		    
		    
		    
		    //PERFORM SENTIMENT ANALYSIS ON NORMALIZED SIRNG AND HASHTAGS .
		    
		    //Basic Positive Emotions Scales
		    String[] Self_assurance= new String[]{"afraid", "scared", "nervous", "jittery", "irritable", "hostile", "guilty", "ashamed", "upset", "distressed"};
		   
		    Long count1=0L;
		    Long count2=0L;
		    for(String s:Self_assurance)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Self-assurance",count1));
		    }
		    if(count2>0l)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Self-assurance",count2));
		    }
		    //Attentiveness		
		    String[] Attentiveness= new String[]{"alert", "attentiveness", "concentrating", "determined"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Attentiveness)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Attentiveness",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Attentiveness",count2));
		    }
		    //Joviality
		    String[] Joviality= new String[]{"happy", "joyful", "delighted", "cheerful", "excited", "enthusiastic", "lively", "energetic"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Joviality)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Joviality",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Joviality",count2));
		    }
		    
		    
		    
		    
		    //General Dimension Scales
		    
		    String[] Negative_Affect= new String[]{"afraid", "scared", "nervous", "jittery", "irritable", "hostile", "guilty", "ashamed", "upset", "distressed"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Negative_Affect)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Negative Affect",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Negative Affect",count2));
		    }
		    
		    String[] Positive_Affet= new String[]{"active"," alert", "attentive", "determined", "enthusiastic", "excited", "inspired", "interested", "pround", "strong"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Positive_Affet)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Positive Affet ",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Positive Affet",count2));
		    }
		    
		    
		    
		    
		    //Basic Negative Emotions Scales
		    
		    String[] Fear= new String[]{"afraid","scared","frightened","nervous","jittery","shaky"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Fear)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Fear",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Fear",count2));
		    }
		    
		    String[] Hostility= new String[]{"angry","hostile","irritable","scornful","disgusted","loathing"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Hostility)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"Hostility",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"Hostility",count2));
		    }
		    
		    String[] Guilt= new String[]{"guilty","ashamed","blameworthy","angry at self","disgusted with self","dissatisfied with self"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Guilt)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"Guilt",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"Guilt",count2));
		    }
		    
		    String[] Sadness= new String[]{"sad","blue","downhearted","alone","lonely"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Sadness)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Sadness",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Sadness",count2));
		    }
		    
		    
		    String[] Shyness= new String[]{"shy","bashful","sheepish","timid"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Shyness)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Shyness",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Shyness",count2));
		    }
		    
		    String[] Fatigue= new String[]{"sleepy","tired","sluggish","drowsy"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Fatigue)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Fatigue",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Fatigue",count2));
		    }
		    
		    String[] Serenity= new String[]{"calm","relaxed","at ease"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Serenity)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Serenity",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Serenity",count2));
		    }
		    
		    
		    String[] Surprise= new String[]{"amazed", "surprised", "astonished"};
			   
		     count1=0L;
		     count2=0L;
		    for(String s:Surprise)
		    {
		    	if (normalised_tweet.contains(s))
		    	{
		    		count1++;
		    	}
		    	if(hashtags.contains(s))
		    	{
		    		count2++;
		    	}
		    	
		    }
		    if(count1>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Surprise",count1));
		    }
		    if(count2>0L)
		    {
		    	sentiment_count_list.add(new Tuple2<>(time+"---Surprise",count2));
		    }
		    
		    
		    
		    
		    //Negative Affect
		    
			
				return sentiment_count_list.iterator();
			
		}).reduceByKey(new Function2<Long, Long, Long>() {
		    @Override
		    public Long call(Long x, Long y) {
		        return x + y;
		    }
		});
		 JavaPairDStream<Long,String> swappedPair=sentiment_to_count.mapToPair(Tuple2::swap);
		 JavaPairDStream<Long,String> sortedStream = swappedPair.transformToPair(s -> s.sortByKey(false));
		 JavaPairDStream<String, Long> original_sentiment_to_count_format= sortedStream.mapToPair(Tuple2::swap);
		 
		 @SuppressWarnings("serial")
		JavaPairDStream<String, String> timeToIndividualSentiment=original_sentiment_to_count_format.mapToPair(x -> {
			
			String[] s=x._1.split("---");
			return new Tuple2<String,String> (s[0],s[1]);
		});
		 JavaPairDStream<String, Iterable<String>> timetoSentimentGroup = timeToIndividualSentiment.groupByKey();
		
		 timetoSentimentGroup.print();
		 timetoSentimentGroup.dstream().saveAsTextFiles(output,"");

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

}
