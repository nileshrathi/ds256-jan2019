package in.ds256.Assignment1;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class wcc {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("WCC");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		JavaPairRDD<String, String> edges = lines.filter(line -> !line.startsWith("#")).mapToPair(line -> parseLine(line)).cache();
		JavaPairRDD<String, Integer> curColor = edges.keys().distinct().mapToPair(v -> new Tuple2<String, Integer>(v, Integer.parseInt(v)));
		int curColorCount = (int) curColor.count();
		while(true) {
			JavaPairRDD<String, Integer> newColor =  edges.join(curColor).values().mapToPair(v -> new Tuple2<String, Integer>(v._1, v._2))
														.reduceByKey((x, y) -> Math.max(x, y));
			newColor = curColor.leftOuterJoin(newColor).mapToPair(p -> new Tuple2<String, Integer>(p._1, Math.max(p._2._1, p._2._2.orElse(0))));
			int newColorCount = (int) newColor.values().distinct().count();
			if (newColorCount == curColorCount) {
				break;
			}
			curColorCount = newColorCount;
			curColor = newColor;
		}

		curColor.saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}

	private static Tuple2<String, String> parseLine(String line) {
		String[] res = line.split("\\s+");
		return new Tuple2<String,String>(res[0], res[1]);
	}
}
