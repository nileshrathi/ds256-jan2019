package in.ds256.Assignment1.Giraph.graphReader;

import in.ds256.Assignment1.Giraph.payLoad.LongPair;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;



public class graphReaderTwo extends TextEdgeInputFormat<LongWritable, NullWritable> {
    private static final Pattern SEPARATOR = Pattern.compile("[ ]");

    public graphReaderTwo() {
    }

    public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new graphReaderTwo.graphReaderTwoReader();
    }

    public class graphReaderTwoReader extends TextEdgeInputFormat<LongWritable, NullWritable>.TextEdgeReaderFromEachLineProcessed<LongPair> {
        public graphReaderTwoReader() {
        }

        protected LongPair preprocessLine(Text line) throws IOException {
            String[] tokens = graphReaderTwo.SEPARATOR.split(line.toString());
            try {
                if (tokens.length >= 2)
                    return new LongPair(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]));
                else
                    return new LongPair(-1L, -1L);  // Add a dummy edge
            }
            catch (NumberFormatException n){
                return new LongPair(-1L, -1L);  // Add a dummy edge
            }

        }

        protected LongWritable getSourceVertexId(LongPair endpoints) throws IOException {
            return new LongWritable(endpoints.getFirst());
        }

        protected LongWritable getTargetVertexId(LongPair endpoints) throws IOException {
            return new LongWritable(endpoints.getSecond());
        }

        protected NullWritable getValue(LongPair endpoints) throws IOException {
            return NullWritable.get();
        }
    }

}
