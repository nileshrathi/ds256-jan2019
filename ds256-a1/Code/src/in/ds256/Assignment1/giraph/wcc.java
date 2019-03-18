package in.ds256.Assignment1.giraph;
import org.apache.giraph.combiner.*;
import org.apache.hadoop.io.WritableComparable;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import scala.collection.parallel.Combiner;
import scala.collection.parallel.ParIterableLike.Max;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.io.IOException;

//VID        , VVal       , EdgeVal     , Message
public class wcc extends BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {

	public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages)
			throws IOException {
		/************** converting to undirected graph ********************/
		if (getSuperstep() == 0) {
			sendMessageToAllEdges(vertex, vertex.getId());
		}

		else if (getSuperstep() == 1) {
			for (IntWritable message : messages) {
				if (vertex.getEdgeValue(message) != NullWritable.get()) {
					vertex.addEdge(EdgeFactory.create(message));
				}
			}
		}
		/*************** converted in 2 supersteps *******************/
		
		  else if (getSuperstep() == 2) { vertex.setValue(vertex.getId());
		  sendMessageToAllEdges(vertex, vertex.getValue()); }
		 
		else {
			//IntWritable maxID = max(messages);
			int maxID = vertex.getValue().get();
			for(IntWritable msg : messages) {
				int ID = msg.get();
				if (ID > maxID) {
					maxID = ID;
				}
			}
			
			if (maxID > vertex.getValue().get()) {
				vertex.setValue(new IntWritable(maxID));
				sendMessageToAllEdges(vertex, new IntWritable(maxID));
			}
		}
		vertex.voteToHalt();
	}
}
