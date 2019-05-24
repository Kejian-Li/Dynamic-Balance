package slb;

import com.google.common.hash.Hashing;
import util.CountEntry;
import util.FrequencyException;
import util.LossyCounting;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class LBDB implements LoadBalancer {

    private List<Server> nodes;
    private int numSources;
    private int serverNum;

    private long loadSamplingGranularity;

    private double delta;  // frequency threshold
    private double imbalance = 0.0f;   // current load imbalance
    private double epsilon;  // load imbalance threshold\
    private double error = delta * 0.1;  // lossy counting error

    private List<LossyCounting> lossyCountings;
    private ArrayList<LocalLoad> localLoads;

    public LBDB(List<Server> nodes, int numSources, double delta, double epsilon) {

        this.nodes = nodes;
        this.serverNum = nodes.size();
        this.numSources = numSources;

        this.delta = delta;
        this.epsilon = epsilon;

        this.loadSamplingGranularity = nodes.get(0).getGranularity();

        for (int i = 1; i < serverNum; i++) {
            assert (loadSamplingGranularity == nodes.get(i).getGranularity());
        }

        this.lossyCountings = new ArrayList<>(numSources);
        localLoads = new ArrayList<>();

        for (int i = 0; i < numSources; i++) {
            lossyCountings.add(new LossyCounting<>(error));
            localLoads.add(new LocalLoad(serverNum));
        }
    }

    private int source = 0; // index of downstream sources: [0, numSources - 1]
    private LossyCounting<Object> lossyCounting;
    private LocalLoad localLoad;

    @Override
    public Server getSever(long timestamp, Object key) {
        localLoad = localLoads.get(source);
        lossyCounting = lossyCountings.get(source);

        int selected; // index of chosen server
        List<CountEntry<Object>> frequentItems = null;
        try {
            lossyCounting.add(key);
            frequentItems = lossyCounting.getFrequentItems(delta);
        }catch (FrequencyException e) {
            System.out.println(e);
        }

        if (!frequentItems.contains(key)) {
            selected = hash(key);
        } else {
            localLoad.addOriginalIndexIntoVk(key);
            imbalance = updateImbalance();
            if (imbalance <= epsilon) {  // current imbalance is less than threshold of imbalance epsilon
                selected = localLoad.findIndexOfMinLoadInVk(key);
            } else {
                selected = localLoad.getIndexOfMinLoad();
                localLoad.addNewIndexIntoVk(key, selected);
            }
        }

        localLoad.increaseCount(selected);

        source++;  // next source to emit the key in round-robin fashion
        if (source == numSources) {
            source = 0;
        }
        return nodes.get(selected);
    }

    private double updateImbalance() {
        double averageCount = lossyCounting.size() / serverNum;
        return (localLoad.getMaxLoad() / averageCount) - 1;  // Load Imbalance: (Max - Avg) / Avg
    }

    private int hash(Object key) {
        return Math.abs(Hashing.murmur3_128().hashBytes(key.toString().getBytes()).asInt() % serverNum);
    }

}
