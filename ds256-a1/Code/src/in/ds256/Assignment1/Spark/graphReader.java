package in.ds256.Assignment1.Spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;

class graphReader {

    static JavaPairRDD<Long, Long> read(String inputFile, JavaSparkContext sc, boolean needUndirected, int numPartitions) {

        JavaPairRDD<Long, Long>  edgeRDD = null;

        switch (inputFile) {
            case "hdfs:///user/ds256/a1/cit-Patents.txt": edgeRDD = readerOne(inputFile, sc, needUndirected, numPartitions); break;
            case "hdfs:///user/ds256/a1/soc-livj.txt": edgeRDD = readerOne(inputFile, sc, needUndirected, numPartitions); break;
            case "hdfs:///user/ds256/a1/orkut.txt": edgeRDD = readerOne(inputFile, sc, needUndirected, numPartitions); break;
            case "hdfs:///user/ds256/a1/USA-road-d.USA.gr": edgeRDD = readerThree(inputFile, sc, needUndirected, numPartitions); break;
            case "hdfs:///user/ds256/a1/gplus.txt": edgeRDD = readerTwo(inputFile, sc, needUndirected, numPartitions); break;
            case "hdfs:///user/ds256/a1/twitter.txt": edgeRDD = readerTwo(inputFile, sc, needUndirected, numPartitions); break;
        }

        return edgeRDD;
    }

    private static JavaPairRDD<Long, Long> readerOne(String inputFile, JavaSparkContext sc, boolean needUndirected, int numPartitions) {

        JavaRDD<String> inputRDD = sc.textFile(inputFile);

        return inputRDD.flatMapToPair(edge -> {
            String[] tokens = edge.split("\t");
            ArrayList<Tuple2<Long, Long>> e = new ArrayList<>();
            if (tokens.length == 2) {
                try {
                    e.add(new Tuple2<> (Long.parseLong(tokens[0]), Long.parseLong(tokens[1])));
                    if (needUndirected)
                        e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[0]))); // Reverse edge
                    else
                        e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[1]))); // To add target vertex in PR
                }
                catch (NumberFormatException n) {return e.iterator();}
            }
            return e.iterator();
        }).repartition(numPartitions).distinct();
    }

    private static JavaPairRDD<Long, Long> readerTwo(String inputFile, JavaSparkContext sc, boolean needUndirected, int numPartitions) {

        JavaRDD<String> inputRDD = sc.textFile(inputFile);

        return inputRDD.flatMapToPair(edge -> {
            String[] tokens = edge.split(" ");
            ArrayList<Tuple2<Long, Long>> e = new ArrayList<>();
            if (tokens.length >= 2) {
                try {
                    e.add(new Tuple2<> (Long.parseLong(tokens[0]), Long.parseLong(tokens[1])));
                    if (needUndirected)
                        e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[0]))); // Reverse edge
                    else
                        e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[1]))); // To add target vertex in PR
                }
                catch (NumberFormatException n) {return e.iterator();}
            }
            return e.iterator();
        }).repartition(numPartitions).distinct();
    }

    private static JavaPairRDD<Long, Long> readerThree(String inputFile, JavaSparkContext sc, boolean needUndirected, int numPartitions) {

        JavaRDD<String> inputRDD = sc.textFile(inputFile);

        return inputRDD.flatMapToPair(edge -> {
            String[] tokens = edge.split(" ");
            ArrayList<Tuple2<Long, Long>> e = new ArrayList<>();
            if (tokens.length >= 2) {
                try {
                    e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[2])));
                    if (needUndirected)
                        e.add(new Tuple2<> (Long.parseLong(tokens[2]), Long.parseLong(tokens[1]))); // Reverse edge
                    else
                        e.add(new Tuple2<> (Long.parseLong(tokens[2]), Long.parseLong(tokens[2]))); // To add target vertex in PR
                }
                catch (NumberFormatException n) {return e.iterator();}
            }
            return e.iterator();
        }).repartition(numPartitions).distinct();
    }

}
