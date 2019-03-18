package in.ds256.Assignment1.giraph.reader;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SimpleEdgeInputFormat extends TextEdgeInputFormat<IntWritable, NullWritable> {

	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public EdgeReader<IntWritable, NullWritable> createEdgeReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException {
		return new SimpleEdgeReader();
  }
    
	public class SimpleEdgeReader extends TextEdgeReader {

		private IntPair processedLine;

		@Override
		public final boolean nextEdge() throws IOException, InterruptedException {
			processedLine = null;
			while(getRecordReader().nextKeyValue() && 
							getRecordReader().getCurrentValue().toString().startsWith("#")) {
					getRecordReader().nextKeyValue();
			}
      return getRecordReader().nextKeyValue();
		}

    protected IntPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new IntPair(Integer.parseInt(tokens[0]),
          Integer.parseInt(tokens[1]));
    }

    protected IntWritable getSourceVertexId(IntPair endpoints)
      throws IOException {
      return new IntWritable(endpoints.getFirst());
    }

    protected IntWritable getTargetVertexId(IntPair endpoints)
      throws IOException {
      return new IntWritable(endpoints.getSecond());
		}
		
    protected NullWritable getValue(IntPair endpoints) throws IOException {
			return NullWritable.get();
		}

		@Override
		public Edge<IntWritable, NullWritable> getCurrentEdge() throws IOException, InterruptedException {
			IntPair processed = processCurrentLine();
      IntWritable targetVertexId = getTargetVertexId(processed);
      NullWritable edgeValue = getValue(processed);
			return EdgeFactory.create(targetVertexId, edgeValue);
		}

		private IntPair processCurrentLine() throws IOException, InterruptedException {
      if (processedLine == null) {
        Text line = getRecordReader().getCurrentValue();
        processedLine = preprocessLine(line);
      }
      return processedLine;
		}

		@Override
		public IntWritable getCurrentSourceId() throws IOException, InterruptedException {
			IntPair processed = processCurrentLine();
			return getSourceVertexId(processed);
		}
	}
}