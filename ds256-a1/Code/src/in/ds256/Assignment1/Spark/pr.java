package in.ds256.Assignment1.Spark;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class pr {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		int numPartitions = Integer.parseInt(args[2]); // No. of partitions
		Double tolerance = Double.parseDouble(args[3]); // Required Tolerance
		Double weight = Double.parseDouble((args[4])); // Weight for PR
		long iterations = 0;

		boolean hasConverged = false;
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// SCHEMA : Tuple2<SourceID, TargetID>
		JavaPairRDD<Long, Long> edgeRDD = graphReader.read(inputFile, sc, false, numPartitions);

		// SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, PR, Old_PR>>
		JavaPairRDD<Long, Tuple3<ArrayList<Long>, Double, Double>> vertexRDD = edgeRDD.groupByKey().mapToPair(vertex ->  {
			ArrayList<Long> adj = new ArrayList<>();
			for(Long v : vertex._2) {
				if (!v.equals(vertex._1))
					adj.add(v);
			}
			return new Tuple2<>(vertex._1, new Tuple3<>(adj, 0.0, 0.0));
		}).repartition(numPartitions).cache();

		long vertexCount = vertexRDD.count();

		vertexRDD = vertexRDD.mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple3<>(vertex._2._1(), 1.0/vertexCount, 1.0/vertexCount))).cache();

		while(! hasConverged) {

			// SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, PR, Old_PR>>
			JavaPairRDD<Long, Tuple3<ArrayList<Long>, Double, Double>> messageRDD = vertexRDD.flatMapToPair(vertex -> {
				ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Double, Double>>> m = new ArrayList<>();
					for (Long v : vertex._2._1()) {
						m.add(new Tuple2<>(v, new Tuple3<>(null, vertex._2._2()/vertex._2._1().size(), 0.0)));
					}
				return m.iterator();
			});

			// SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, PR, Old_PR>>
			vertexRDD = vertexRDD.union(messageRDD).groupByKey().coalesce(numPartitions).mapToPair(vertex -> {
				Double sum = 0.0;
				Double pr = 0.0;
				Double old_pr;
				ArrayList<Long> adjList = new ArrayList<>();
				for(Tuple3<ArrayList<Long>, Double, Double> val: vertex._2) {
					if(val._1() == null)
						sum += val._2();
					else {
						adjList = val._1();
						pr = val._2();
					}

				}
				old_pr = pr;
				pr = (sum * weight) + ((1- weight) / vertexCount);
				return new Tuple2<>(vertex._1, new Tuple3<>(adjList, pr, old_pr));
			}).cache();

			hasConverged = vertexRDD.map(vertex -> Math.abs(vertex._2._2() - vertex._2._3()) / vertex._2._3()).reduce(Double::max) <= tolerance;

			iterations++;

		}

		// Write Output
		vertexRDD.mapToPair(vertex -> new Tuple2<>(vertex._1, vertex._2._2())).sortByKey().saveAsTextFile(outputFile);

		System.out.println("Logger: Shriram.Ramesh - Iterations: " + iterations);
		
		sc.stop();
		sc.close();
		
	}
}
