package in.ds256.Assignment1;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class pr {

	public static Double CONVERGENCE_THRESHOLD = 0.001d;

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		JavaPairRDD<String, String> edges = lines.filter(line -> !line.startsWith("#")).mapToPair(line -> parseLine(line)).cache();
		JavaPairRDD<String, Integer> edgeCount = edges.mapValues(v -> 1).reduceByKey((x, y) -> (x + y)).cache();
		JavaRDD<String> keys = edges.keys().distinct().cache();
		long num_keys = keys.count();
		Double initialValue = 1.0d / num_keys;
		JavaPairRDD<String, Double> curRank = keys.mapToPair(v -> new Tuple2<String, Double>(v, initialValue));
		keys.unpersist();

		while(true) {
			JavaPairRDD<String, Double> messages = curRank.join(edgeCount).mapToPair(p -> new Tuple2<String, Double>(p._1, p._2._1 / p._2._2));
			JavaPairRDD<String, Double> newRank =  edges.join(messages).values().mapToPair(v -> new Tuple2<String, Double>(v._1, v._2))
														.reduceByKey((x, y) -> (x+y)).mapValues(v -> v * 0.85d + 0.15d / num_keys);
			newRank = curRank.leftOuterJoin(newRank).mapToPair(p -> new Tuple2<String, Double>(p._1, p._2._2.orElse(p._2._1)));
			double change = curRank.join(newRank).values().map(l -> Math.abs(l._1 - l._2)).reduce((x, y) -> Math.max(x, y));
			curRank = newRank;
			if (change < CONVERGENCE_THRESHOLD) {
				break;
			}
		}

		curRank.saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}

	private static Tuple2<String, String> parseLine(String line) {
		String[] res = line.split("\\s+");
		return new Tuple2<String,String>(res[0], res[1]);
	}
}
