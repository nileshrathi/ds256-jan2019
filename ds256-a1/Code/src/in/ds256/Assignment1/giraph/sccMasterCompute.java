package in.ds256.Assignment1.giraph;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class sccMasterCompute extends DefaultMasterCompute {

    public static final String AGGREGATOR_PHASE = "app.phase";
    public static final String AGGREGATOR_FWD_DONE = "app.fwddone";
    public static final String AGGREGATOR_BWD_DONE = "app.bwddone";

    public static final int PHASE_TRANSPOSE = 0;
    public static final int PHASE_TRIM = 1;
    public static final int PHASE_FWD_TRAVERSAL = 2;
    public static final int PHASE_BWD_TRAVERSAL_START = 3;
    public static final int PHASE_BWD_TRAVERSAL_REST = 4;

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerPersistentAggregator(AGGREGATOR_PHASE, IntOverwriteAggregator.class);
        registerAggregator(AGGREGATOR_FWD_DONE, BooleanAndAggregator.class);
        registerAggregator(AGGREGATOR_BWD_DONE, BooleanAndAggregator.class);
    }

    @Override
    public void compute() {
        if(getSuperstep() == 0) {
            setPhase(PHASE_TRANSPOSE);
        } else {
            int phase = getPhase();
            switch(phase) {
                case PHASE_TRANSPOSE:
                    setPhase(PHASE_TRIM);
                    break;
                case PHASE_TRIM:
                    setPhase(PHASE_FWD_TRAVERSAL);
                    break;
                case PHASE_FWD_TRAVERSAL:
                    boolean fwdDone 
                        = ((BooleanWritable)getAggregatedValue(AGGREGATOR_FWD_DONE)).get();
                    if(fwdDone) {
                        setPhase(PHASE_BWD_TRAVERSAL_START);
                    }
                    break;
                case PHASE_BWD_TRAVERSAL_START:
                    setPhase(PHASE_BWD_TRAVERSAL_REST);
                    break;
                case PHASE_BWD_TRAVERSAL_REST:
                    boolean bwdDone 
                        = ((BooleanWritable)getAggregatedValue(AGGREGATOR_BWD_DONE)).get();
                    if (bwdDone) {
                        setPhase(PHASE_TRANSPOSE);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private int getPhase() {
        return ((IntWritable)getAggregatedValue(AGGREGATOR_PHASE)).get();
    }

    private void setPhase(int phase) {
        setAggregatedValue(AGGREGATOR_PHASE, new IntWritable(phase));
    }

}