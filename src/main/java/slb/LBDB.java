package slb;

import com.google.common.hash.Hashing;
import sun.plugin.dom.exception.InvalidStateException;
import util.FrequencyException;
import util.LossyCounting;

import java.util.*;


public class LBDB implements LoadBalancer {

    private List<Server> nodes;
    private int numSources;
    private int serverNum;

    private long loadSamplingGranularity;

    private long[][] localWorkload;

    private long elementCount[]; // count of element in upstream sources

    private float delta;  // frequency threshold
    private float imbalance = 0.0f;   // current load imbalance
    private float epsilon;  // load imbalance threshold\
    private double error = delta * 0.1;  // lossy counting error

    private List<LossyCounting> lossyCountings;
    private ArrayList<HashMap<Object, HashSet<Integer>>> lists;

    public LBDB(List<Server> nodes, int numSources, float delta, float epsilon) {

        this.nodes = nodes;
        this.serverNum = nodes.size();
        this.numSources = numSources;

        this.delta = delta;
        this.epsilon = epsilon;

        this.loadSamplingGranularity = nodes.get(0).getGranularity();

        for (int i = 1; i < serverNum; i++) {
            assert (loadSamplingGranularity == nodes.get(i).getGranularity());
        }

        this.localWorkload = new long[numSources][];  // upstream nodes workload
        for (int i = 0; i < numSources; i++) {
            localWorkload[i] = new long[serverNum];
        }

        this.elementCount = new long[numSources];

        this.lossyCountings = new ArrayList<>(numSources);
        lists = new ArrayList<>(); // for Vk;
        for (int i = 0; i < numSources; i++) {
            lossyCountings.add(new LossyCounting<>(error));
            lists.add(new HashMap<Object, HashSet<Integer>>());
        }
    }

    private int source = 0; // index of downstream sources: [0, numSources - 1]
    private LossyCounting<Object> lossyCounting;

    @Override
    public Server getSever(long timestamp, Object key) {
        // emulate to emit the key from downstream sources in round-robin fashion
        elementCount[source]++;
        source++;
        if (source == numSources) {
            source = 0;
        }

        int selected; // index of chosen server
        float f = getFrequency(key);

        if (f <= delta * serverNum) {
            selected = Math.abs(Hashing.murmur3_128().hashBytes(key.toString().getBytes()).asInt() % serverNum);
            addKeyIntoVk(key, selected);
        } else {
            imbalance = computeCurrentImbalance();
            if (imbalance <= epsilon) {
                selected = findLeastLoadOneInVk();
            } else {
                selected = findLeastLoadOneInV();
                addKeyIntoVk(key, selected);
            }
        }

        localWorkload[source][selected]++;
        return nodes.get(selected);
    }

    private HashMap<Object, HashSet<Integer>> map; // for Vk: key => set(index of server)
    private HashSet<Integer> Vk;

    // add selected index into Vk
    private void addKeyIntoVk(Object key, int selected) {
        map = lists.get(source);
        Vk = map.get(key);
        if (Vk == null) {
            Vk = new HashSet<>();
            map.put(key, Vk);
        }
        Vk.add(selected);
    }

    private float getFrequency(Object key) {
        float f = 0.0f; // frequency of key
        lossyCounting = lossyCountings.get(source);
        try {
            lossyCounting.add(key);
            f = lossyCounting.estimateCount(key) / elementCount[source];
        } catch (FrequencyException e) {
            System.out.println(e);
        }
        return f;
    }

    private float computeCurrentImbalance() {
        long totalLoad = 0;
        for (int i = 0; i < serverNum; i++) {
            totalLoad += localWorkload[source][i];
        }
        long averageLoad = totalLoad / serverNum;
        long maxLoad = findMaxLoadOneInV();
        return maxLoad / averageLoad - 1;  // Load Imbalance definition: (Max - Avg) / Avg = (Max / AVg) - 1
    }

    private int findLeastLoadOneInVk() {  //Vk: received key
        int min = 0;
        Iterator<Integer> it = Vk.iterator();
        if (Vk.size() < 1) {
            throw new InvalidStateException("Vk is null");
        }
        long minOne = localWorkload[source][it.next()];

        int temp;
        while (it.hasNext()) {
            temp = it.next();
            if (localWorkload[source][temp] < minOne) {
                minOne = localWorkload[source][temp];
                min = temp;
            }
        }
        return min;
    }

    private int findMaxLoadOneInV() {
        int max = 0;
        long maxOne = localWorkload[source][0];
        for (int i = 1; i < serverNum; i++) {
            if (localWorkload[source][i] > maxOne) {
                maxOne = localWorkload[source][i];
                max = i;
            }
        }
        return max;
    }

    private int findLeastLoadOneInV() {  // V: all of downstream servers
        int min = 0;
        long minOne = localWorkload[source][0];
        for (int i = 1; i < serverNum; i++) {
            if (localWorkload[source][i] < minOne) {
                minOne = localWorkload[source][i];
                min = i;
            }
        }
        return min;
    }

}
