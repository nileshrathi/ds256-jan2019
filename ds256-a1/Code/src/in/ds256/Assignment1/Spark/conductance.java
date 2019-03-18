package in.ds256.Assignment1.Spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class conductance {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS
        int numPartitions = Integer.parseInt(args[2]); // No. of partitions
        long iterations = 0;

        Random rand = new Random();

        SparkConf sparkConf = new SparkConf().setAppName("Conductance");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // SCHEMA : Tuple2<SourceID, TargetID>
        JavaPairRDD<Long, Long> edgeRDD = graphReader.read(inputFile, sc, true, numPartitions);

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isSet, degree>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> vertexRDD = edgeRDD.groupByKey().mapToPair(vertex -> {
            ArrayList<Long> adjList = Lists.newArrayList(vertex._2);
            return new Tuple2<>(vertex._1, new Tuple3<>(adjList, (rand.nextInt(1000) % 3) == 0, (long) adjList.size()));
        }).repartition(numPartitions).cache();

        // Get IN and OUT Degree
        List<Tuple2<Boolean, Long>> inOutDegree;
        inOutDegree = vertexRDD.mapToPair(vertex -> new Tuple2<>(vertex._2._2(), vertex._2._3())).reduceByKey((_1, _2) -> _1 + _2).sortByKey().collect();

        // Get Cross Edges
        // SCHEMA : Tuple2<TargetID, Tuple3<null, isSet, SourceID>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> messageRDD = vertexRDD.flatMapToPair(vertex -> {
            ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>> m = new ArrayList<>();
            for(Long val: vertex._2._1()) {
                if (val < vertex._1)
                    m.add(new Tuple2<>(val, new Tuple3<>(null, vertex._2._2(), vertex._1)));
            }
            return m.iterator();
        });

        Long crossCount = vertexRDD.union(messageRDD).groupByKey().map(vertex -> {
            Long countTrue = 0L;
            Long countFalse = 0L;
            Boolean myValue = true;
            for(Tuple3<ArrayList<Long>, Boolean, Long> val: vertex._2) {
                if(val._1() == null) {
                    if (val._2()) {
                        countTrue++;
                    }
                    else
                        countFalse++;
                }
                else {
                    myValue = val._2();
                }
            }
            return myValue?countFalse:countTrue;
        }).reduce((_1, _2) -> _1 + _2);

        // Compute Conductance
        Long m = (inOutDegree.get(1)._2 < inOutDegree.get(0)._2) ? inOutDegree.get(1)._2 : inOutDegree.get(0)._2;
        Double conductance;
        if (m==0L) {
            conductance =  (crossCount == 0L ? 0.0 : Double.POSITIVE_INFINITY);
        }
        else
            conductance = crossCount * 1.0 / m;

        // Write Output
        vertexRDD.sortByKey().map(vertex -> vertex._1.toString() + ',' + vertex._2._2().toString() + ',' + conductance.toString()).saveAsTextFile(outputFile);

        System.out.println("Logger: Shriram.Ramesh - Iterations: " + iterations);

        sc.stop();
        sc.close();

    }
}
