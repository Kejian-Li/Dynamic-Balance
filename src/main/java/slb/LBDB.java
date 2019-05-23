package slb;

import com.clearspring.analytics.stream.StreamSummary;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.lossycounting.LossyCountingModel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LBDB implements LoadBalancer {

    private List<Server> nodes;
    private int numSources;
    private float delta;  // frequency threshold
    private float epsilon;  // load imbalance threshold
    private int serverNum;

    private long loadSamplingGranularity;

    private long[][] localWorkload;

    private Map<Integer, StreamSummary<String>> map;

    private HashFunction hash;

    private long messageCount[];

    private float imbalance = 0.0001f;   // current load imbalance

    private LossyCountingModel<Object> model;

    public LBDB(List<Server> nodes, int numSources, int delta, float epsilon) {

        this.nodes = nodes;
        this.numSources = numSources;
        this.delta = delta;
        this.epsilon = epsilon;

        this.serverNum = nodes.size();
        this.loadSamplingGranularity = nodes.get(0).getGranularity();

        for (int i = 1; i < serverNum; i++) {
            assert (loadSamplingGranularity == nodes.get(i).getGranularity());
        }

        this.localWorkload = new long[numSources][];  // upstream nodes workload
        for (int i = 0; i < numSources; i++) {
            localWorkload[i] = new long[nodes.size()];
        }

        this.map = new HashMap<>();
        for (int i = 0; i < numSources; i++) {
            map.put(i, new StreamSummary<String>(Constants.STREAM_SUMMARY_CAPACITY)); // frequent keys in upstream nodes
        }

        this.messageCount = new long[numSources];

        hash = Hashing.murmur3_128();

        this.model = new LossyCountingModel<>(delta, error);
    }

    private long totalElement = 0;
    private long averageLoad = 0;
    private long maxLoad;

    private double error = 0.1 * delta;


    @Override
    public Server getSever(long timestamp, Object key) {
        totalElement++;
        Arrays.sort(messageCount);
        maxLoad = messageCount[serverNum - 1];
        averageLoad = totalElement / serverNum;
        imbalance = (maxLoad / averageLoad) - 1;  // definition of Load Imbalance: LI = (max - avg) / avg

        int i; // index of chosen downstream parallel server

        float f = 0.0f;   //////////////////// get frequency of given key

        if (f <= delta * serverNum) {
            i = Math.abs(hash.hashBytes(key.toString().getBytes()).asInt() % serverNum);
        } else if (imbalance <= epsilon) {
            i = findLeastLoadOneInVk();
        } else {
            i = findLeastLoadOneInV();
        }
        return nodes.get(i);
    }

    private int findLeastLoadOneInVk() {  //Vk is set of servers which received key k
        return 0;
    }

    private int findLeastLoadOneInV() {
        return 0;
    }

}
