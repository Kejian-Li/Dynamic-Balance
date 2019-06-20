package slb2;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import util.cardinality.Hash;
import util.cardinality.HyperLogLog;
import util.cardinality.MurmurHash;
import util.load.FrequencyException;
import util.load.LossyCounting;

import java.util.Collection;
import java.util.Iterator;


public class HolisticPartitioner extends AbstractPartitioner implements GetStatistics {

    private int numServers;
    private float delta;
    private double error;  // lossy counting error
    private float epsilon;  // default = 10^-4

    private Hash hash;
//    private LossyCounting<Integer> lossyCounting;

    private LossyCounting<Integer> lossyCounting;

    private long[] localLoad;               // record downstream load
    private HyperLogLog totalCardinality;
    private HyperLogLog[] localCardinality; // record downstream cardinality

    private Multimap<Integer, Integer> Vk;

    private final static int DEFAULT_LOG2M = 24; // 12 for 10^7 keys of 32 bits

    public HolisticPartitioner() {
        super();
    }

    public HolisticPartitioner(int numServers, float delta) {

        this.numServers = numServers;
        this.delta = delta;
        this.error = delta * 0.1;
        this.epsilon = 0.01f;

        localLoad = new long[numServers];

        hash = MurmurHash.getInstance();
        lossyCounting = new LossyCounting<>(error);

        totalCardinality = new HyperLogLog(DEFAULT_LOG2M);
        localCardinality = new HyperLogLog[numServers];
        for (int i = 0; i < numServers; i++) {
            localCardinality[i] = new HyperLogLog(DEFAULT_LOG2M);
        }

        Vk = HashMultimap.create();
    }

    private int x;
    private long estimatedCount;
    private double estimatedFrequency;

    @Override
    public int partition(Object key) {

        int selected;

        x = Integer.parseInt(key.toString());   // for zipf data

        // for statistics
        totalCardinality.offer(x);

        try {
            lossyCounting.add(x);
        } catch (FrequencyException e) {
            e.printStackTrace();
        }

        estimatedCount = lossyCounting.estimateCount(x);

        estimatedFrequency = (double) estimatedCount / lossyCounting.size();

        if (estimatedFrequency <= delta) {
            selected = hash(x);
        } else {
            float Im = updateCurrentPartialLoadImbalance();
            if (Im < epsilon) {
                selected = findLeastLoadOneInVk(x);
            } else {
                selected = findLeastLoadOneInV();
                Vk.put(x, selected);
            }
        }

        localLoad[selected]++;

        // for statistics
        localCardinality[selected].offer(x);

        return selected;
    }

    private float updateCurrentPartialLoadImbalance() {
        float averageLoad = (lossyCounting.size()  - 1) / (float) numServers;
        return averageLoad == 0 ? 0.0f : (getCumulativeAverageLoadOfWorkersFor(x) - averageLoad) / averageLoad;
    }

    private long getCumulativeAverageLoadOfWorkersFor(int x) {
        Collection<Integer> values = Vk.get(x);
        if (values.isEmpty()) {
            return localLoad[hash(x)];
        }
        Iterator<Integer> it = values.iterator();
        long load = 0;
        while (it.hasNext()) {
            load += localLoad[it.next()];
        }
        return load / values.size();
    }

    private int findLeastLoadOneInV() {
        int min = 0;
        for (int i = 1; i < numServers; i++) {
            if (localLoad[i] < localLoad[min]) {
                min = i;
            }
        }
        return min;
    }

    private int findLeastLoadOneInVk(int x) {
        int min = -1;
        long minOne = Integer.MAX_VALUE;
        Collection<Integer> values = Vk.get(x);
        if (values.isEmpty()) {
            int hashed = hash(x);
            Vk.put(x, hashed);
            return hashed;
        }
        Iterator<Integer> it = values.iterator();
        int i;
        while (it.hasNext()) {
            i = it.next();
            if (localLoad[i] < minOne) {
                minOne = localLoad[i];
                min = i;
            }
        }
        return min;
    }

    private int hash(Object key) {
        return Math.abs(hash.hash(key)) % numServers;
    }

    @Override
    public String getName() {
        return "Holistic";
    }

    @Override
    public long getTotalCardinality() {
        return totalCardinality.cardinality();
    }

    @Override
    public Multimap<Integer, Integer> getVk() {
        return Vk;
    }
}

