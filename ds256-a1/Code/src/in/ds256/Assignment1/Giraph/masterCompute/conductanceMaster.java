package in.ds256.Assignment1.Giraph.masterCompute;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class conductanceMaster extends MasterCompute {

    public void initialize() throws IllegalAccessException, InstantiationException { // Register aggregators
        registerPersistentAggregator("InDegree", LongSumAggregator.class);
        registerPersistentAggregator("OutDegree", LongSumAggregator.class);
        registerPersistentAggregator("crossEdges", LongSumAggregator.class);
    }

    public void compute() {

        if (getSuperstep() == 0) { // Init Aggregators
            setAggregatedValue("InDegree", new LongWritable(0));
            setAggregatedValue("OutDegree", new LongWritable(0));
            setAggregatedValue("crossEdges", new LongWritable(0));
        }
        else if(getSuperstep() == 5) { // Compute conductance
            long inDegree = ((LongWritable) getAggregatedValue("InDegree")).get();
            long outDegree = ((LongWritable) getAggregatedValue("OutDegree")).get();
            long crossEdges = ((LongWritable) getAggregatedValue("crossEdges")).get();
            double conductance;
            long m = (inDegree < outDegree) ? inDegree : outDegree;
            if (m==0L) {
                conductance =  (crossEdges == 0L ? 0.0 : Double.POSITIVE_INFINITY);
            }
            else
                conductance = crossEdges * 1.0 / m;
            System.out.println("Conductance (Computed): " + conductance);
        }

    }

    public void write(DataOutput var1) throws IOException {

    };

    public void readFields(DataInput var1) throws IOException {

    };

}
