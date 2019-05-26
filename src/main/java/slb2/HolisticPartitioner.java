package slb2;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.hash.Hashing;
import util.cardinality.HyperLogLog;
import util.load.CountEntry;
import util.load.FrequencyException;
import util.load.LossyCounting;

import java.util.List;


public class HolisticPartitioner implements StreamPartitioner {

    private int numServers;

    private long totalLoad;
    private long[] localLoad;  // local load for downstream servers


    private float delta;  // default = 0.2
    private double error;  // lossy counting error
    private float alpha;  // load and cardinality balance factor, default = 0.5

    private LossyCounting lossyCounting;

    private HyperLogLog totalCardinality;
    private HyperLogLog[] localCardinality; // for upstream nodes

    private final static int DEFAULT_LOG2M = 12;

    public HolisticPartitioner(int numServers, float delta, float alpha) {

        this.numServers = numServers;
        this.delta = delta;
        this.error = delta * 0.1;
        this.alpha = alpha;

        totalLoad = 0;
        localLoad = new long[numServers];

        lossyCounting = new LossyCounting(error);

        totalCardinality = new HyperLogLog(DEFAULT_LOG2M);
        localCardinality = new HyperLogLog[numServers];
        for (int i = 0; i < numServers; i++) {
            localCardinality[i] = new HyperLogLog(DEFAULT_LOG2M);
        }
    }

    private double[] scores = new double[numServers];  // reuse for each incoming element

    @Override
    public int partition(Object key) throws Exception {
        int selected;

        List<CountEntry<Object>> frequentItems;
        try {
            lossyCounting.add(key);
            frequentItems = lossyCounting.getFrequentItems(delta);
        } catch (FrequencyException e) {
            throw e;
        }

        if (!frequentItems.contains(key)) {
            selected = hash(key);
        } else {
            for (int i = 0; i < numServers; i++) {
                scores[i] = getScore(i, key);
            }
            selected = findIndexOfMaxScore();
        }

        totalLoad++;
        totalCardinality.offer(key);
        localLoad[selected]++;
        localCardinality[selected].offer(key);

        return selected;
    }

    private int indicator;
    private HyperLogLog tempHyperLogLog;

    private double getScore(int i, Object key) throws Exception {
        try {
            tempHyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
            tempHyperLogLog.addAll(localCardinality[i]);
        } catch (CardinalityMergeException e) {
            throw e;
        }
        if (tempHyperLogLog.offer(key)) {  // affect cardinality( do not contain this key before): there is precision
            indicator = 0;
        } else {  // unaffect cardinality
            indicator = 1;
        }
        tempHyperLogLog = null; // for GC
        return indicator + alpha * (1 - localCardinality[i].cardinality() / totalCardinality.cardinality())
                + (1 - alpha) * (1 - localLoad[i] / totalLoad);
    }

    private int findIndexOfMaxScore() {
        int maxIndex = 0;
        double maxScore = scores[0];
        for (int i = 0; i < numServers; i++) {
            if (maxScore < scores[i]) {
                maxScore = scores[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    private int hash(Object key) {
        return Math.abs(Hashing.murmur3_128().hashBytes(key.toString().getBytes()).asInt() % numServers);
    }

}
