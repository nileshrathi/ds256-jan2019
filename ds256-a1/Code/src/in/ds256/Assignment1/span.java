package in.ds256.Assignment1;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class span {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		int source =  Integer.parseInt(args[2]);
		
		SparkConf sparkConf = new SparkConf().setAppName("Span");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		JavaPairRDD<Integer, Integer> edges = lines.filter(line -> !line.startsWith("#")).mapToPair(line -> parseLine(line)).cache();
		JavaPairRDD<Integer, Integer> parents = edges.keys().distinct().mapToPair(v -> new Tuple2<Integer, Integer>(v, v==source?source:-1));
		int count = (int) parents.filter(p -> p._2 == -1).count();
		while(true) {
			JavaPairRDD<Integer, Integer> newParents =  edges.join(parents).filter(f -> f._2._2 != -1)
						.mapToPair(v -> new Tuple2<Integer, Integer>(v._2._1, v._1))
						.reduceByKey((x, y) -> Math.max(x, y));
			parents = parents.leftOuterJoin(newParents).mapToPair(
					p -> new Tuple2<Integer, Integer>(p._1, (p._2._1 == -1)? p._2._2.orElse(-1): p._2._1));
			int newCount = (int) parents.filter(p -> p._2 == -1).count();
			if (newCount == count) {
				break;
			}
			count = newCount;
		}

		parents.saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}

	private static Tuple2<Integer, Integer> parseLine(String line) {
		String[] res = line.split("\\s+");
		return new Tuple2<Integer,Integer>(Integer.parseInt(res[0]), Integer.parseInt(res[1]));
	}
}
