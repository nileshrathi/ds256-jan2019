package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.graph.Computation;


import java.io.IOException;

public class wcc extends DefaultMasterCompute{
	@Override
	public final void compute(){
	long superstep = getSuperstep();
	if(superstep==0) {
		setComputation(directedToUndirected.class);
	}
	else {
	setComputation(connectedComponents.class);
	}
	}	
}