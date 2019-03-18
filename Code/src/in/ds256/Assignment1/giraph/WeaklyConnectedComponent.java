package in.ds256.Assignment1.giraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.BasicComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

/**Reference https://github.com/Sotera/distributed-graph-analytics/blob/master/dga-giraph/src/main/java/com/soteradefense/dga/wcc/WeaklyConnectedComponentComputation.java **/
/** https://github.com/apache/giraph/blob/0c1e2ebce6e4f6e1a47d4dec5c0b8475433ef8df/giraph-examples/src/main/java/org/apache/giraph/examples/ConnectedComponentsComputation.java **/
/** https://github.com/Sotera/distributed-graph-analytics/blob/master/dga-giraph/src/main/java/com/soteradefense/dga/wcc/WeaklyConnectedComponentComputation.java **/

public class WeaklyConnectedComponent extends  BasicComputation<
LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	 
	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,Iterable<DoubleWritable> messages) 
	{
		double maxID = -1; /** No edge id in the input was -1 */
		if(getSuperstep() == 0)	{
			vertex.setValue(new DoubleWritable((double)(vertex.getId().get())));
			sendMessageToAllEdges(vertex, vertex.getValue());
		}
		else {
			
			maxID =vertex.getValue().get();
			
			for(DoubleWritable message: messages) {
				 if(maxID < message.get()) 
					 maxID = message.get();
			}
			
			if(maxID > vertex.getValue().get())
			{
				vertex.setValue(new DoubleWritable(maxID));
				sendMessageToAllEdges(vertex, vertex.getValue());
			}	
			else
				vertex.voteToHalt();
		}
		System.out.println("Weakly connected Component Superstep: "+getSuperstep());
		vertex.voteToHalt();
	}
}