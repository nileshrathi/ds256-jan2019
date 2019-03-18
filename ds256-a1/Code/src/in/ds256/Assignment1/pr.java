package in.ds256.Assignment1;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public class pr {
	
	private static class Sum implements Function2<Double, Double, Double> {
	    @Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }
	  }
	private static final Pattern SPACES = Pattern.compile("\\s+");

	public static void main(String[] args) throws IOException {
		
		
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);//.sample(true, 0.1);
		

	    JavaPairRDD<String, Iterable<String>> links = inputRDD.mapToPair(s -> {
	      String[] parts = SPACES.split(s);
	      return new Tuple2<>(parts[0], parts[1]);
	    }).distinct().groupByKey().cache();

	    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

	    //for loop is configurable for convergence rate
	    for (int current = 0; current < 10; current++) {
	      // Calculates URL contributions to the rank of other URLs.
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

	    // Collects all URL ranks and dump them to console.
	    ranks.saveAsTextFile(outputFile);
	    
	    //following code was used when file was too big to fit in hdfs like for gplus and twitter graph
		/*
		 * PrintStream out = null; try { out = new PrintStream(new
		 * FileOutputStream(outputFile));
		 * 
		 * } catch (FileNotFoundException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } System.setOut(out);
		 * System.out.println(ranks.take(1000));
		 */
		sc.stop();
		sc.close();
		
	}
}
