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

import java.io.IOException;

public class pr extends BasicComputation<LongWritable, DoubleWritable, NullWritable, DoubleWritable> {
	
	private static double dumping_factor=0.85;
	
	private static int lim_supersteps = 5;

	//private static final Logger LOG = Logger.getLogger(pr.class);

	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, NullWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		if (getSuperstep()==0) {
			vertex.setValue(new DoubleWritable(1));
		}
		double sum = 0;
		for (DoubleWritable msg : messages) {
			sum += msg.get();
		}
		vertex.setValue( new DoubleWritable((1-dumping_factor) + (dumping_factor) * sum));
		if (getSuperstep() < lim_supersteps) {
			sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()));
		} else {
			vertex.voteToHalt();
		}
	}
}