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
import org.apache.giraph.combiner.MessageCombiner;


import org.apache.giraph.master.DefaultMasterCompute;

import java.io.IOException;



public class connectedComponents extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

	
	long cur_comp;
	LongWritable cur_msg;
	@Override
	public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,Iterable<LongWritable> messages) {
	 
	if(getSuperstep()==0) {
		vertex.setValue(vertex.getId());
		sendMessageToAllEdges(vertex, vertex.getId());
	}
	for(LongWritable msg : messages) {
		cur_comp = msg.get();
		long comp=vertex.getValue().get();
		if (comp<cur_comp) {
			cur_comp=comp;
		}
	}
	vertex.setValue(new LongWritable(cur_comp));
    sendMessageToAllEdges(vertex,new LongWritable(cur_comp));
	vertex.voteToHalt();
	}
}
