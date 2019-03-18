package in.ds256.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class pr {

	public static int NUM_ITERATIONS = 20;
	public static double EPSILON = 0.01;

	/**
	 * Returns either if the iterations have expired or has converged, faster of the
	 * 2
	 * 
	 * @return true of either converged or number of iterations are expired
	 */
	public boolean hasConverged(int iterCount) {

		boolean result = false;

		if (iterCount >= NUM_ITERATIONS)
			result = true;

		return result;

	}

	public static void main(String[] args) throws IOException {

		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS

		System.out.println("the input and outfput file are " + inputFile + " " + outputFile);

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("PageRank"); /** configuration to run on local desktop **/
//		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);

		inputRDD = inputRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {			
								
				if(v1.contains("#"))
					return false;			
				
				return true;
			}
		});
		
		JavaPairRDD<String, String> edgeRDD = inputRDD.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(String t) throws Exception {

				ArrayList<Tuple2<String, String>> myIter = new ArrayList<Tuple2<String, String>>();

				System.out.println("The string is " + t);
				String[] vertices = t.split("\\s+");

				Tuple2<String, String> myTuple = new Tuple2<String, String>(vertices[0], vertices[1]);
				myIter.add(myTuple);

				return myIter.iterator();

			}
		});

		/** Get the distinct set of users from the EdgeRDD **/
		JavaPairRDD<String, Iterable<String>> groupedUsersRDD = edgeRDD.distinct().groupByKey().cache(); // don't know
																											// why do we
																											// need
																											// distinct
																											// here

		System.out.println("The groupedUsersRDD count is " + groupedUsersRDD.count());

		JavaPairRDD<String, Tuple2<Double, Boolean>> initRankRDD = groupedUsersRDD
				.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Tuple2<Double, Boolean>>() {

					@Override
					public Tuple2<String, Tuple2<Double, Boolean>> call(Tuple2<String, Iterable<String>> t)
							throws Exception {

						Double rank = 1.0;
						String vertexId = t._1();
						Boolean changed = true;

						Tuple2<Double, Boolean> myVal = new Tuple2<Double, Boolean>(rank, changed);
						Tuple2<String, Tuple2<Double, Boolean>> myTuple = new Tuple2<String, Tuple2<Double, Boolean>>(
								vertexId, myVal);

						return myTuple;
					}

				});
		
		JavaPairRDD<String, Tuple2<Double, Boolean>> prevRankRDD; 

		for (int i = 0; i < NUM_ITERATIONS; i++) {
			
			/** Asign init to prev **/
			prevRankRDD = initRankRDD;

			/** Now we have per UserID number of urls and rank **/
			JavaPairRDD<String, Tuple2<Iterable<String>, Tuple2<Double, Boolean>>> joinEdgesRDD = groupedUsersRDD
					.join(initRankRDD);

			for (Tuple2<String, Tuple2<Iterable<String>, Tuple2<Double, Boolean>>> singleTuple : joinEdgesRDD
					.collect()) {
				System.out.println("The tuples are " + singleTuple.toString());
			}

			JavaPairRDD<String, Tuple2<Double, Boolean>> pageRankRDD = joinEdgesRDD.values().flatMapToPair(
					new PairFlatMapFunction<Tuple2<Iterable<String>, Tuple2<Double, Boolean>>, String, Tuple2<Double, Boolean>>() {

						@Override
						public Iterator<Tuple2<String, Tuple2<Double, Boolean>>> call(
								Tuple2<Iterable<String>, Tuple2<Double, Boolean>> t) throws Exception {

							int outlinks = Iterables.size(t._1());
							Double oldRank = t._2()._1();
							Boolean changed = t._2()._2();

							ArrayList<Tuple2<String, Tuple2<Double, Boolean>>> myIter = new ArrayList<Tuple2<String, Tuple2<Double, Boolean>>>();

							for (String vertexId : t._1()) {
								Double newRank = oldRank / outlinks;

								Double diff = Math.abs(newRank - oldRank);
								System.out.println("The outlinks are "+outlinks);
								System.out.println("rank is "+newRank);
								if (diff <= EPSILON)
									changed = false;
								else
									changed = true;

								Tuple2<Double, Boolean> myVal = new Tuple2<Double, Boolean>(newRank, changed);
								Tuple2<String, Tuple2<Double, Boolean>> myTuple = new Tuple2<String, Tuple2<Double, Boolean>>(
										vertexId, myVal);

								myIter.add(myTuple);
							}

							return myIter.iterator();
						}
					});

			/** Add the page rank values of all the neighbors **/
			initRankRDD = pageRankRDD.reduceByKey(
					new Function2<Tuple2<Double, Boolean>, Tuple2<Double, Boolean>, Tuple2<Double, Boolean>>() {

						@Override
						public Tuple2<Double, Boolean> call(Tuple2<Double, Boolean> v1, Tuple2<Double, Boolean> v2)
								throws Exception {

							Double rank = v1._1() + v2._1();
							Boolean changed = v1._2() || v2._2();

							Tuple2<Double, Boolean> myTuple = new Tuple2<Double, Boolean>(rank, changed);

							return myTuple;
						}
					});

			/** Split the page-rank with respect to dampening factor **/
			initRankRDD = initRankRDD.mapValues(new Function<Tuple2<Double, Boolean>, Tuple2<Double, Boolean>>() {

				@Override
				public Tuple2<Double, Boolean> call(Tuple2<Double, Boolean> v1) throws Exception {

					Double rank = 0.15 + 0.85 * v1._1();
					Boolean changed = v1._2();

					Tuple2<Double, Boolean> myTuple = new Tuple2<Double, Boolean>(rank, changed);
					return myTuple;
				}
			});

			/** We can see from this whether we have converged or not **/
			JavaPairRDD<String, Tuple2<Tuple2<Double, Boolean>, Tuple2<Double, Boolean>>> convergenceRDD = initRankRDD.join(prevRankRDD);
			
			JavaPairRDD<Boolean,Double> finalRDD = convergenceRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<Tuple2<Double,Boolean>,Tuple2<Double,Boolean>>>, Boolean, Double>() {

				@Override
				public Tuple2<Boolean, Double> call(
						Tuple2<String, Tuple2<Tuple2<Double, Boolean>, Tuple2<Double, Boolean>>> t) throws Exception {

					Tuple2<Boolean,Double> myTuple;
					Double newRank = t._2()._1()._1();
					Double oldRank = t._2()._2()._1();
					
					Boolean changed = true;
					Double diff = Math.abs(newRank - oldRank);
					
					if(diff<=EPSILON)
						changed = false;
					
					myTuple = new Tuple2<Boolean, Double>(changed, diff);/** key can either be true or false **/
					
					return myTuple;
				}
			});
			
			finalRDD = finalRDD.reduceByKey(new Function2<Double, Double, Double>() {
				
				@Override
				public Double call(Double v1, Double v2) throws Exception {
					// TODO Auto-generated method stub
					return v1+v2;
				}
			});
			
			if(finalRDD.keys().collect().size()==1 && finalRDD.keys().collect().get(0)==false) {
				System.out.println("Things have converged :) ");
				break;
			}
			
					
		} /** End of while loop **/

		/** Save the RDD into a file **/
		initRankRDD.saveAsTextFile(outputFile);

		sc.stop();
		sc.close();

	}
}