package in.ds256.Assignment1;

import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class pr {
	private static final Pattern SPACES = Pattern.compile("\\s+");
	  private static class Sum implements Function2<Double, Double, Double> {
	    @Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }
	  }

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; 
		String outputFile = args[1];
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);
		
		if (args.length < 2) {
		      System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
		      System.exit(1);
		    }

		  
		    JavaPairRDD<String, Iterable<String>> links = inputRDD.mapToPair(s -> {
		      String[] parts = SPACES.split(s);
		      return new Tuple2<>(parts[0], parts[1]);
		    }).distinct().groupByKey().cache();

		    
		    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

		    
		    for (int current = 0; current < Integer.parseInt(args[2]); current++) {
		      
		      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
		        .flatMapToPair(s -> {
		          int urlCount = Iterables.size(s._1());
		          List<Tuple2<String, Double>> results = new ArrayList<>();
		          for (String n : s._1) {
		            results.add(new Tuple2<>(n, s._2() / urlCount));
		          }
		          return results.iterator();
		        });

		      
		      ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
		    }

		   ranks.saveAsTextFile(outputFile);
		
		
		sc.stop();
		sc.close();
		
	}
}
