package slb;

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
    private long[][] localLoad;  // local load for sources

    private long loadSamplingGranularity;

    private float delta;  // default = 0.2
    private double error;  // lossy counting error
    private float alpha;  // load and cardinality balance factor, default = 0.5

    private List<LossyCounting> lossyCountings;
    private ArrayList<HyperLogLog[]> lists;
    private final static double DEFAULT_STANDARD_DEVIATION = 0.4;  

    public LBDB(List<Server> nodes, int numSources, float delta, float alpha) {

        this.nodes = nodes;
        this.serverNum = nodes.size();
        this.numSources = numSources;
        this.delta = delta;
        this.error = delta * 0.1;
        this.alpha = alpha;

        localLoad = new long[numSources][serverNum];

        this.loadSamplingGranularity = nodes.get(0).getGranularity();

        for (int i = 1; i < serverNum; i++) {
            assert (loadSamplingGranularity == nodes.get(i).getGranularity());
        }

        this.lossyCountings = new ArrayList<>(numSources);
        this.lists = new ArrayList<>();

        for (int i = 0; i < numSources; i++) {
            lossyCountings.add(new LossyCounting<>(error));
            HyperLogLog[] hyperLogLogs = new HyperLogLog[serverNum];
            for (int j = 0; j < serverNum; j++) {
                hyperLogLogs[j] = new HyperLogLog(DEFAULT_STANDARD_DEVIATION);
            }
            lists.add(hyperLogLogs);
        }
    }

    private int source = 0; // index of downstream sources: [0, numSources - 1]
    private LossyCounting<Object> lossyCounting;
    private HyperLogLog[] hyperLogLogs;
    private double[] scores = new double[serverNum];  // reuse for each incoming element


    @Override
    public Server getSever(long timestamp, Object key) {
        lossyCounting = lossyCountings.get(source);
        hyperLogLogs = lists.get(source);

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
                scores[i] = getScore(i);
            }
            selected = findIndexOfMaxScore();
        }

        localLoad[source][selected]++;  // update local load for selected node
        hyperLogLogs[selected].offer(key);  // update cardinality for selected node

        source++;  // next source to emit the key in round-robin fashion
        if (source == numSources) {
            source = 0;
        }
        return nodes.get(selected);
    }

    private double getScore(int i) {
        return alpha * hyperLogLogs[i].cardinality() + (1 - alpha) * localLoad[source][i];
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
