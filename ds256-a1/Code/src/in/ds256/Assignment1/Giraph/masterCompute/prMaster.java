package in.ds256.Assignment1.Giraph.masterCompute;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class prMaster extends MasterCompute {

    public void initialize() throws IllegalAccessException, InstantiationException { // Register aggregators
        registerPersistentAggregator("relDiff", DoubleMaxAggregator.class);
        registerPersistentAggregator("sumPR", DoubleSumAggregator.class);
    }

    public void compute() {

        if (getSuperstep() == 0) { // Init Aggregators
            setAggregatedValue("relDiff", new DoubleWritable(0.0));
            setAggregatedValue("sumPR", new DoubleWritable(0.0));
        }
        else if(getSuperstep() >= 4) { // Determine if converged
            double relDiff = ((DoubleWritable) getAggregatedValue("relDiff")).get();
            double sumPR = ((DoubleWritable) getAggregatedValue("sumPR")).get();
            double tolerance = Double.parseDouble(getContext().getConfiguration().get("tolerance"));
            if (relDiff <=  tolerance) {
                System.out.println("Sum of PR (Computed): " + sumPR); // Debug
                haltComputation();
            }
            else
                setAggregatedValue("relDiff", new DoubleWritable(0.0));
                setAggregatedValue("sumPR", new DoubleWritable(0.0));
        }

    }

    public void write(DataOutput var1) throws IOException {

    };

    public void readFields(DataInput var1) throws IOException {

    };

}
