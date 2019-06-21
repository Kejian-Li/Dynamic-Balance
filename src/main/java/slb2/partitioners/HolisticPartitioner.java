package slb2.partitioners;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import slb2.GetStatistics;
import util.cardinality.Hash;
import util.cardinality.MurmurHash;
import util.load.FrequencyException;
import util.load.LossyCounting;

import java.util.Collection;
import java.util.Iterator;


/**
 * Class for Zipf data set whose elements are numbers.
 */
public class HolisticPartitioner extends AbstractPartitioner implements GetStatistics {

    private int numServers;
    private float delta;
    private double error;  // lossy counting error
    private float epsilon;  // default = 10^-4

    private Hash hash;

    private LossyCounting<Integer> lossyCounting;

    private long[] localLoad;               // record downstream load

    private Multimap<Integer, Integer> Vk;

    private final static int DEFAULT_LOG2M = 12; // 12 for 10^7 keys of 32 bits

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

        Vk = HashMultimap.create();
    }

    private int x;
    private long estimatedCount;
    private double estimatedFrequency;

    @Override
    public int partition(Object key) {

        int selected;

        x = Integer.parseInt(key.toString());   // for zipf data

        add(x);

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
            float RIm = updateRegionalLoadImbalance(x);
            if (RIm < epsilon) {
                selected = findLeastLoadOneInVk(x);
            } else {
                selected = findLeastLoadOneInV();
                Vk.put(x, selected);
            }
        }

        localLoad[selected]++;

        return selected;
    }

    private float updateRegionalLoadImbalance(int x) {
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
    public Multimap<Integer, Integer> getVk() {
        return Vk;
    }
}

