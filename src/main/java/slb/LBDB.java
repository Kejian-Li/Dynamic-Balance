package slb;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.hash.Hashing;
import util.cardinality.HyperLogLog;
import util.load.CountEntry;
import util.load.FrequencyException;
import util.load.LossyCounting;

import java.util.ArrayList;
import java.util.List;


public class LBDB implements LoadBalancer {

    private List<Server> nodes;
    private int numSources;
    private int serverNum;

    private long[] totalLoad; // record total load through every upstream source
    private long[][] localLoad;  // local load for downstream servers

    private long loadSamplingGranularity;

    private float delta;  // default = 0.2
    private double error;  // lossy counting error
    private float alpha;  // load and cardinality balance factor, default = 0.5

    private List<LossyCounting> lossyCountings;
    private HyperLogLog[] totalCardinality; // for upstream nodes
    private ArrayList<HyperLogLog[]> cardinalityLists;  // for downstream servers
    private final static int DEFAULT_LOG2M = 12;

    public LBDB(List<Server> nodes, int numSources, float delta, float alpha) {

        this.nodes = nodes;
        this.serverNum = nodes.size();
        this.numSources = numSources;
        this.delta = delta;
        this.error = delta * 0.1;
        this.alpha = alpha;

        totalLoad = new long[numSources];
        localLoad = new long[numSources][serverNum];

        this.loadSamplingGranularity = nodes.get(0).getGranularity();

        for (int i = 1; i < serverNum; i++) {
            assert (loadSamplingGranularity == nodes.get(i).getGranularity());
        }

        this.lossyCountings = new ArrayList<>(numSources);
        totalCardinality = new HyperLogLog[numSources];
        this.cardinalityLists = new ArrayList<>();

        for (int i = 0; i < numSources; i++) {
            lossyCountings.add(new LossyCounting<>(error));
            totalCardinality[i] = new HyperLogLog(DEFAULT_LOG2M);

            HyperLogLog[] hyperLogLogs = new HyperLogLog[serverNum];
            for (int j = 0; j < serverNum; j++) {
                hyperLogLogs[j] = new HyperLogLog(DEFAULT_LOG2M);
            }
            cardinalityLists.add(hyperLogLogs);
        }
    }

    private int source = 0; // index of downstream sources: [0, numSources - 1]
    private LossyCounting<Object> lossyCounting;
    private HyperLogLog[] hyperLogLogs;
    private double[] scores = new double[serverNum];  // reuse for each incoming element

    @Override
    public Server getSever(long timestamp, Object key) {
        lossyCounting = lossyCountings.get(source);
        hyperLogLogs = cardinalityLists.get(source);

        totalLoad[source]++;  // record total load
        totalCardinality[source].offer(key); // record cardinality through this source

        int selected; // index of chosen server
        List<CountEntry<Object>> frequentItems = null;
        try {
            lossyCounting.add(key);
            frequentItems = lossyCounting.getFrequentItems(delta);
        } catch (FrequencyException e) {
            System.out.println(e);
        }

        if (!frequentItems.contains(key)) {
            selected = hash(key);
        } else {
            for (int i = 0; i < serverNum; i++) {
                scores[i] = getScore(i, key);
            }
            selected = findIndexOfMaxScore();
        }

        localLoad[source][selected]++;  // update local load for selected node
        hyperLogLogs[selected].offer(key);  // update cardinality for selected node and return false

        source++;  // next source to emit the key in round-robin fashion
        if (source == numSources) {
            source = 0;
        }
        return nodes.get(selected);
    }

    private int indicator;
    private HyperLogLog tempHyperLogLog;

    private double getScore(int i, Object key) {
        try {
            tempHyperLogLog = new HyperLogLog(DEFAULT_LOG2M);
            tempHyperLogLog.addAll(hyperLogLogs[i]);
        } catch (CardinalityMergeException e) {
            System.out.println(e);
        }
        if (tempHyperLogLog.offer(key)) {  // affect cardinality( do not contain this key before): there is precision
            indicator = 0;
        } else {  // unaffect cardinality
            indicator = 1;
        }
        tempHyperLogLog = null; // for GC
        return indicator + alpha * (1 - hyperLogLogs[i].cardinality() / totalCardinality[source].cardinality())
                + (1 - alpha) * (1 - localLoad[source][i] / totalLoad[source]);
    }

    private int findIndexOfMaxScore() {
        int maxIndex = 0;
        double maxScore = scores[0];
        for (int i = 0; i < serverNum; i++) {
            if (maxScore < scores[i]) {
                maxScore = scores[i];
                maxIndex = i;
            }
        }
        return maxIndex;
    }

    private int hash(Object key) {
        return Math.abs(Hashing.murmur3_128().hashBytes(key.toString().getBytes()).asInt() % serverNum);
    }

}
