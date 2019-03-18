package in.ds256.Assignment1.Spark;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

public class span {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		int numPartitions = Integer.parseInt(args[2]); // No. of partitions
		Long sourceVertex = Long.parseLong(args[3]); // Source Vertex
		long iterations = 0;

		boolean hasConverged = false;

		SparkConf sparkConf = new SparkConf().setAppName("Span");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// SCHEMA : Tuple2<SourceID, TargetID>
		JavaPairRDD<Long, Long> edgeRDD = graphReader.read(inputFile, sc, true, numPartitions);

		// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
		JavaPairRDD<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>> vertexRDD = edgeRDD.groupByKey().mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple4<>(Lists.newArrayList(vertex._2), !(vertex._1.equals(sourceVertex)), (vertex._1.equals(sourceVertex))?0:Long.MAX_VALUE, -1L))).repartition(numPartitions).cache();

		while(!hasConverged) {

			// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
			JavaPairRDD<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>> messageRDD = vertexRDD.filter(vertex -> !(vertex._2._2())).flatMapToPair(vertex -> {
				ArrayList<Tuple2<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>>> m = new ArrayList<>();
					for (Long v : vertex._2._1()) {
						m.add(new Tuple2<>(v, new Tuple4<>(null, false, vertex._2._3(), vertex._1)));
					}
				return m.iterator();
			});

			// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
			vertexRDD = vertexRDD.union(messageRDD).groupByKey().coalesce(numPartitions).mapToPair(vertex -> {
				Long minimumValue = Long.MAX_VALUE;
				Long parentVertex = -1L;
				Long currentValue = 0L;
				Long currentParentVertex = -1L;
				boolean hasHalted = true;
				ArrayList<Long> adjList = new ArrayList<>();
				for(Tuple4<ArrayList<Long>, Boolean, Long, Long> val: vertex._2) {
					if (! (val._1() == null)) {
						currentValue = val._3();
						adjList = val._1();
						currentParentVertex = val._4();
					}
					else if (val._3() + 1 < minimumValue) {
						minimumValue = val._3() + 1;
						parentVertex = val._4();
					}
				}
				if (minimumValue < currentValue) {
					hasHalted = false;
					currentValue = minimumValue;
					currentParentVertex = parentVertex;
				}
				return new Tuple2<>(vertex._1, new Tuple4<>(adjList, hasHalted, currentValue, currentParentVertex));
			}).cache();

			hasConverged = vertexRDD.filter(vertex -> !(vertex._2._2())).take(1).isEmpty();

			iterations++;

		}

		// Write Output
		vertexRDD.map(vertex -> vertex._1.toString() + "," + vertex._2._3().toString() + "," + vertex._2._4().toString()).saveAsTextFile(outputFile);

		System.out.println("Logger: Shriram.Ramesh - Iterations: " + iterations);
		
		sc.stop();
		sc.close();
		
	}
}
