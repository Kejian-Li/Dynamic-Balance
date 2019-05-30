package slb2;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import util.cardinality.Hash;
import util.cardinality.HyperLogLog;
import util.cardinality.MurmurHash;
import util.load.CountEntry;
import util.load.FrequencyException;
import util.load.LossyCounting;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class HolisticPartitioner implements StreamPartitioner {

    private int numServers;
    private float delta;  // default = 0.2
    private double error;  // lossy counting error
    private float alpha;  // load and cardinality balance factor, default = 0.5

    private Hash hash;
    private LossyCounting<String> lossyCounting;

    private long totalLoad;
    private long[] localLoad;               // record downstream load
    private HyperLogLog totalCardinality;
    private HyperLogLog[] localCardinality; // record downstream cardinality
    private double[] scores;  // reuse for each incoming element

    private final static int DEFAULT_LOG2M = 24; // 12 for 10^7 keys of 32 bits

    public HolisticPartitioner(int numServers, float delta, float alpha) {

        this.numServers = numServers;
        this.delta = delta;
        this.error = delta * 0.1;
        this.alpha = alpha;

        totalLoad = 0;
        localLoad = new long[numServers];

        hash = MurmurHash.getInstance();
        lossyCounting = new LossyCounting<>(error);

        totalCardinality = new HyperLogLog(DEFAULT_LOG2M);
        localCardinality = new HyperLogLog[numServers];
        for (int i = 0; i < numServers; i++) {
            localCardinality[i] = new HyperLogLog(DEFAULT_LOG2M);
        }

        scores = new double[numServers];
    }

    @Override
    public int partition(Object key) {
        int selected;

        // update total
        totalLoad++;
        totalCardinality.offer(key);

        try {
            lossyCounting.add(key.toString());
        }catch (FrequencyException e) {
            e.printStackTrace();
        }
        Set<String> frequentItems = getFrequentItems(lossyCounting, delta, totalLoad);

        if (!frequentItems.contains(key.toString())) {
            selected = hash(key);
        } else {
            for (int i = 0; i < numServers; i++) {
                scores[i] = computeScore(i, key);
            }
            selected = findIndexOfMaxScore();
        }

        // update local
        localLoad[selected]++;
        localCardinality[selected].offer(key);

        return selected;
    }

    private Set<String> getFrequentItems(LossyCounting<String> lossyCounting, float probability, long totalLoad) {
        Set<String> frequentItems = new HashSet<>();
        List<CountEntry<String>> counters = lossyCounting.getFrequentItems();
        for (CountEntry<String> counter : counters) {
            if (counter.getFrequency() / totalLoad > probability) {
                frequentItems.add(counter.getItem());
            }
        }
        return frequentItems;
    }

    private int indicator = 0;
    private HyperLogLog tempHyperLogLog;

    private double computeScore(int i, Object key) {
        try {
            tempHyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
            tempHyperLogLog.addAll(localCardinality[i]);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
        if (tempHyperLogLog.offer(key)) {  // affect cardinality
            indicator = 0;
        } else {                           // unaffect cardinality
            indicator = 1;
        }
        tempHyperLogLog = null; // for GC

        double averageCardinality = totalCardinality.cardinality() / (double) numServers;
        double cardinalityPart = (averageCardinality - localCardinality[i].cardinality()) / averageCardinality;

        double averageLoad = totalLoad / (double) numServers;
        double loadPart = (averageLoad - localLoad[i]) / averageLoad;

        return indicator + alpha * cardinalityPart + (1 - alpha) * loadPart;
    }

    private int findIndexOfMaxScore() {
        int maxIndex = 0;
        double maxScore = scores[0];
        for (int i = 1; i < numServers; i++) {
            if (maxScore < scores[i]) {
                maxScore = scores[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    private int hash(Object key) {
        return Math.abs(hash.hash(key) % numServers);
    }

    @Override
    public String getName() {
        return "Holistic";
    }
}
