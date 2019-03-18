package in.ds256.Assignment1.Spark;

import java.io.IOException;
import java.util.ArrayList;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class wcc {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS
        int numPartitions = Integer.parseInt(args[2]); // No. of partitions
        long iterations = 0;

        boolean hasConverged = false;

        SparkConf sparkConf = new SparkConf().setAppName("WCC");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // SCHEMA : Tuple2<SourceID, TargetID>
        JavaPairRDD<Long, Long> edgeRDD = graphReader.read(inputFile, sc, true, numPartitions);

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> vertexRDD = edgeRDD.groupByKey().mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple3<>(Lists.newArrayList(vertex._2), true, vertex._1))).repartition(numPartitions).cache();

        while(!hasConverged) {

           // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
           JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> messageRDD = vertexRDD.flatMapToPair(vertex -> {
               ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>> m = new ArrayList<>();
               if (vertex._2._2()) {
                   for (Long v : vertex._2._1()) {
                       m.add(new Tuple2<>(v, new Tuple3<>(null, true, vertex._2._3())));
                   }
               }
               return m.iterator();
           });

           // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
           vertexRDD = vertexRDD.union(messageRDD).groupByKey().coalesce(numPartitions).mapToPair(vertex -> {
               Long maximumValue = 0L;
               Long currentValue = 0L;
               boolean hasChanged = false;
               ArrayList<Long> adjList = new ArrayList<>();
               for(Tuple3<ArrayList<Long>, Boolean, Long> val: vertex._2) {
                   if (! (val._1() == null)) {
                       currentValue = val._3();
                       adjList = val._1();
                   }
                   if (val._3() > maximumValue) {
                       maximumValue = val._3();
                   }
               }
               if (maximumValue > currentValue) {
                   hasChanged = true;
                   currentValue = maximumValue;
               }
               return new Tuple2<>(vertex._1, new Tuple3<>(adjList, hasChanged, currentValue));
           }).cache();

           hasConverged = vertexRDD.filter(vertex -> vertex._2._2()).take(1).isEmpty();

           iterations++;

        }

        // Write Output
        vertexRDD.mapToPair(vertex ->  new Tuple2<>(vertex._2._3(), vertex._1)).sortByKey().saveAsTextFile(outputFile);

        System.out.println("Logger: Shriram.Ramesh - Iterations: " + iterations);

        sc.stop();
        sc.close();

    }
}
