package in.ds256.Assignment1;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class scc {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setAppName("WCC");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile(inputFile);
		JavaPairRDD<String, String> edges = lines.filter(line -> !line.startsWith("#")).mapToPair(line -> parseLine(line)).cache();
		JavaPairRDD<String, String> reverseEdges = edges.mapToPair(edge -> new Tuple2<String, String>(edge._2, edge._1)).cache();
		JavaPairRDD<String, Integer> result = edges.keys().distinct().mapToPair(v -> new Tuple2<String, Integer>(v, -1));
		int resultCount = (int) result.filter(p -> p._2 == -1).count();

		while(true) {
			JavaPairRDD<String, Integer> trimmedVertices = edges.subtractByKey(reverseEdges).keys().distinct()
															.mapToPair(v -> new Tuple2<String, Integer>(v, Integer.parseInt(v)));
			result = result.leftOuterJoin(trimmedVertices).mapToPair(
				p -> new Tuple2<String, Integer>(p._1, (p._2._1 == -1)? p._2._2.orElse(-1): p._2._1));
			JavaPairRDD<String, String> newEdges = edges.subtractByKey(trimmedVertices).cache();
			JavaPairRDD<String, String> newReverseEdges = reverseEdges.subtractByKey(trimmedVertices).cache();
			edges.unpersist();
			reverseEdges.unpersist();
			edges = newEdges;
			reverseEdges = newReverseEdges;

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
			curColor.cache();

			JavaPairRDD<String, Boolean> curSCCVertices = curColor.mapToPair(p -> new Tuple2<String, Boolean>(p._1, (Integer.parseInt(p._1) == p._2)));
			int curReduceSum = curSCCVertices.values().map(v -> v?1:0).reduce((x, y) -> (x + y));
			while(true) {
				JavaPairRDD<String, Boolean> newSCCVertices = reverseEdges.join(curSCCVertices).filter(f -> f._2._2)
																.mapToPair(v -> new Tuple2<String, Boolean>(v._2._1, v._2._2))
																.reduceByKey((x, y) -> x || y);
				newSCCVertices = curSCCVertices.leftOuterJoin(newSCCVertices).mapToPair(
					p -> new Tuple2<String, Boolean>(p._1, (!p._2._1)? p._2._2.orElse(false): p._2._1));
				int newReduceSum = newSCCVertices.values().map(v -> v?1:0).reduce((x, y) -> (x + y));
				if (newReduceSum == curReduceSum) {
					break;
				}
				curSCCVertices = newSCCVertices;
				curReduceSum = newReduceSum;
			}

			curSCCVertices = curSCCVertices.filter(f -> f._2);
			JavaPairRDD<String, Integer> sccVertices = curColor.join(curSCCVertices)
														.mapToPair(v -> new Tuple2<String, Integer>(v._1, v._2._1));
			curColor.unpersist();
			
			result = result.leftOuterJoin(sccVertices).mapToPair(
				p -> new Tuple2<String, Integer>(p._1, (p._2._1 == -1)? p._2._2.orElse(-1): p._2._1));
			int newResultCount = (int) result.filter(p -> p._2 == -1).count();
			if (resultCount == newResultCount) {
				break;
			}
			resultCount = newResultCount;
			System.err.println("DEBUG : " + resultCount);

			newEdges = edges.subtractByKey(sccVertices).cache();
			newReverseEdges = reverseEdges.subtractByKey(sccVertices).cache();
			edges.unpersist();
			reverseEdges.unpersist();
			edges = newEdges;
			reverseEdges = newReverseEdges;
		}

		result.saveAsTextFile(outputFile);
	
		sc.stop();
		sc.close();
		
	}

	private static Tuple2<String, String> parseLine(String line) {
		String[] res = line.split("\\s+");
		return new Tuple2<String,String>(res[0], res[1]);
	}
}
