package in.ds256.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import com.google.common.collect.Iterables;

import scala.Tuple2;
import scala.Tuple3;

public class wcc {
	
	public static int NUM_PARTITIONS = 8;

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WCC");
//		SparkConf sparkConf = new SparkConf().setAppName("WCC");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile).repartition(4); /**enforce 4 partitions **/

		inputRDD = inputRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {			
								
				if(v1.contains("#"))
					return false;			
				
				return true;
			}
		});
		
		JavaPairRDD<Long, Long> forwardRDD ;
		
		/** forwardRDD is the undirected edge list **/
		forwardRDD = inputRDD.mapToPair( new PairFunction<String, Long, Long>() {

			@Override
			public Tuple2<Long, Long> call(String t) throws Exception {
				String row = t;					
				String[] vertices = row.split("\\s+");

				Tuple2<Long, Long> forward = new Tuple2<Long, Long>(Long.valueOf(vertices[0]), Long.valueOf(vertices[1]));
				
				return forward;
			}
		} ).partitionBy(new HashPartitioner(NUM_PARTITIONS));
		
		
		
		JavaPairRDD<Long,Long> backwardRDD = forwardRDD.mapToPair( new PairFunction<Tuple2<Long,Long>, Long, Long>() {

			@Override
			public Tuple2<Long, Long> call(Tuple2<Long, Long> t) throws Exception {
				
				Tuple2<Long, Long> backward = new Tuple2<Long, Long>(t._2(), t._1());				
				return backward;
				
			}
		} ).partitionBy(new HashPartitioner(NUM_PARTITIONS));
		
//		System.out.println("The backwardRDD count is "+backwardRDD.count());
		
		forwardRDD = forwardRDD.union(backwardRDD);
		
//		System.out.println("The forwardRDD count is "+forwardRDD.count());
	
		JavaPairRDD<Long, Tuple3<Long, Long, Boolean>> graphRDD = forwardRDD.mapToPair(  new PairFunction<Tuple2<Long,Long>, Long, Tuple3<Long, Long, Boolean>>() {

			@Override
			public Tuple2<Long, Tuple3<Long, Long, Boolean>> call(Tuple2<Long, Long> t) throws Exception {

				Tuple2<Long, Long> row = t;
				
				Long vertexId = row._1();
				Long neighborVertexId = row._2();
				Boolean changed = true;
				
				Long maxValue = vertexId>neighborVertexId? vertexId:neighborVertexId;
//				System.out.println("the vertex1 is "+vertexId+" The vertex2 is "+neighborVertexId);
				
				Tuple3<Long, Long, Boolean> myTupleVal = new Tuple3<Long, Long, Boolean>(neighborVertexId, maxValue, changed); /** Saving the max value */
				Tuple2<Long,Tuple3<Long, Long, Boolean>> myTuple = new Tuple2<Long, Tuple3<Long,Long,Boolean>>(vertexId, myTupleVal);
				
				return myTuple;
				
			}
		}  );
		
//		System.out.println("The (before) number of entries in "+graphRDD.count());

		
		int i =0;
		JavaPairRDD<Long, Tuple3<Long, Long, Boolean>> prevGraphRDD;
		
		while(true) {
			
			prevGraphRDD = graphRDD;
			
			i++;					
			JavaPairRDD<Long, Tuple3<Long, Long, Boolean>> messagesRDD = graphRDD.partitionBy(new HashPartitioner(NUM_PARTITIONS)).mapToPair(new PairFunction<Tuple2<Long,Tuple3<Long,Long,Boolean>>, Long, Tuple3<Long, Long, Boolean>>() {

				@Override
				public Tuple2<Long, Tuple3<Long, Long, Boolean>> call(Tuple2<Long, Tuple3<Long, Long, Boolean>> t)
						throws Exception {
					
					Tuple3<Long, Long, Boolean> neighbor = t._2();
					Long neighborVertexId = neighbor._1();
					Long maxValue = neighbor._2(); //This is what is being sent as a message
					Boolean changed = neighbor._3(); 
					Long vertexId = t._1();
					
					Tuple3<Long, Long, Boolean> myTupleVal = new Tuple3<Long, Long, Boolean>(vertexId, maxValue, changed); /** neighbor vertex Id is null in the message, this is what differentiates between message and the graph RDD **/
					Tuple2<Long, Tuple3<Long, Long, Boolean>> myTuple = new Tuple2<Long, Tuple3<Long,Long,Boolean>>(neighborVertexId, myTupleVal);
					
					return myTuple;
				}
			});			
			
//			System.out.println("The message sent is "+messagesRDD.count());
			
			graphRDD = graphRDD.union(messagesRDD).distinct().groupByKey().mapPartitionsToPair( new PairFlatMapFunction<Iterator<Tuple2<Long,Iterable<Tuple3<Long,Long,Boolean>>>>, Long, Tuple3<Long, Long, Boolean>>() {
				@Override
				public Iterator<Tuple2<Long, Tuple3<Long, Long, Boolean>>> call(Iterator<Tuple2<Long, Iterable<Tuple3<Long, Long, Boolean>>>> t) throws Exception {
					
					ArrayList<Tuple2<Long, Tuple3<Long, Long, Boolean>>> myIter = new ArrayList<Tuple2<Long,Tuple3<Long,Long,Boolean>>>();					
					
					while(t.hasNext()) {
						
						Tuple2<Long, Iterable<Tuple3<Long, Long, Boolean>>> row = t.next();
						
						Long vertexId = row._1();
						Long maxVal = Long.MIN_VALUE;
						Boolean changed = false;
						
						Iterator<Tuple3<Long, Long, Boolean>> neighbors = row._2().iterator(); /** neighbors **/
						
						/** get the actual value sent by the vertex **/
						while(neighbors.hasNext()) {							
							Tuple3<Long, Long, Boolean> message = neighbors.next();
							
							if(message._2()!=null) { /** trying to find the max value sent by neighbors **/
								if(maxVal < message._2()) {
									maxVal = message._2();
									changed = true; /** Important condition which decides whether the code will converge or not **/
								}
							}							
						}
						
//						System.out.println("The maxval is "+maxVal);
						
						/** Prepare tuples for neighbors for the vertex**/					
						neighbors = row._2().iterator();
						while(neighbors.hasNext()) {						
							 Tuple3<Long, Long, Boolean> message = neighbors.next();
							 
							 if(message._1()!=null) { /** vertex's actual neighbor **/								 
								 Tuple3<Long, Long, Boolean> neighborTuple = new Tuple3<Long, Long, Boolean>(message._1(), maxVal, changed); /** message._1() is the actual neighbor vertex for the vertexId **/
								 Tuple2<Long, Tuple3<Long, Long, Boolean>> myTuple = new Tuple2<Long, Tuple3<Long,Long,Boolean>>(vertexId, neighborTuple);								 
								 myIter.add(myTuple);
							 }							
						}												
					}					
					return myIter.iterator();					
				}
				
			}).partitionBy(new HashPartitioner(NUM_PARTITIONS));
						
//			System.out.println("The number of entries in currRDD "+graphRDD.count());	
//			System.out.println("The number of entries in prevRDD "+prevGraphRDD.count());
			
			if(i>1) {
				JavaPairRDD<Long, Tuple2<Tuple3<Long, Long, Boolean>, Tuple3<Long, Long, Boolean>>>  convergenceRDD = graphRDD.join(prevGraphRDD);
				
				JavaPairRDD<Boolean, Long> finalRDD = convergenceRDD.mapToPair( new PairFunction<Tuple2<Long,Tuple2<Tuple3<Long,Long,Boolean>,Tuple3<Long,Long,Boolean>>>, Boolean, Long>() {

					@Override
					public Tuple2<Boolean, Long> call(
							Tuple2<Long, Tuple2<Tuple3<Long, Long, Boolean>, Tuple3<Long, Long, Boolean>>> t)
							throws Exception {
					
						Tuple2<Long, Tuple2<Tuple3<Long, Long, Boolean>, Tuple3<Long, Long, Boolean>>> row = t;
						Boolean changed = true;
						
						Tuple3<Long, Long, Boolean> firstOne = row._2()._1();
						Tuple3<Long, Long, Boolean> secondOne = row._2()._2();
						
						Long val1 = firstOne._2();
						Long val2 = secondOne._2();
						
						if(val1.equals(val2))
							changed = false;
												
						Tuple2<Boolean, Long> myTuple = new Tuple2<Boolean, Long>(changed, (long) 0);
						
						return myTuple;
						
					}
				}  );
				
//				System.out.println("The number of entries in "+graphRDD.count());	
//				System.out.println("The number of entries in prevRDD "+prevGraphRDD.count());
				
//				for(Tuple2<Boolean,Long> item : finalRDD.collect() ) {
//					System.out.println("The key is "+item._1());
//				}
				
				finalRDD = finalRDD.reduceByKey( new Function2<Long, Long, Long>() {
					
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
				} );
				
				if(finalRDD.keys().collect().size()==1 && finalRDD.keys().collect().get(0)==false) {
					System.out.println("Things have converged :) ");
					break;
				}
	
			}
			
									
		}/* end of while loop */
		System.out.println("The number of iterations "+i);
		graphRDD.saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}
}
