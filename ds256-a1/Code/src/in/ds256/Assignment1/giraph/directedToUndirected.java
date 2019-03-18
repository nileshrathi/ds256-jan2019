package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.apache.giraph.edge.EdgeFactory;

import org.apache.giraph.master.DefaultMasterCompute;

import java.io.IOException;

public class directedToUndirected extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

	@Override
	public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,Iterable<LongWritable> messages) {
	 
	if(getSuperstep()==0) {
		sendMessageToAllEdges(vertex, vertex.getId());
	}
	for (LongWritable msg : messages){
		LongWritable id = new LongWritable(msg.get());
		if (vertex.getEdgeValue(id)==NullWritable.get()) {
			vertex.addEdge(EdgeFactory.create(id,NullWritable.get()));
		}
	}
	vertex.voteToHalt();
	}
}
