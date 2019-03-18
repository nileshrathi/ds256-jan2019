package in.ds256.Assignment1.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;

public class pr extends BasicComputation<IntWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	private static int MAX_SUPERSTEPS = 5;

	private static final Logger LOG = Logger.getLogger(pr.class);
	
	@Override
	public void compute(Vertex<IntWritable, DoubleWritable, FloatWritable> vertex, Iterable<DoubleWritable> messages)
			throws IOException {
		if(getSuperstep()==0)
		{
			//double a =1;
			DoubleWritable vertexValue=new DoubleWritable(1/getTotalNumVertices());
			//vertexValue.set(MAX_SUPERSTEPS);
			vertex.setValue(vertexValue);
		}
		
		if (getSuperstep() >= 1) {
			double sum = 0;
			for (DoubleWritable message : messages) {
				sum += message.get();
			}
			DoubleWritable vertexValue = new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
			vertex.setValue(vertexValue);
		}

		if (getSuperstep() < MAX_SUPERSTEPS) {
			//System.out.println(vertex.getId()+" "+vertex.getValue());
			long edges = vertex.getNumEdges();
			sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get() / edges));
		} 
		
		/*if (getSuperstep() == 0)
			System.out.println("wow one step");*/
		
		vertex.voteToHalt();
		
	}
}