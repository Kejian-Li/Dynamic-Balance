package slb;

public interface MyLoadBalancer extends LoadBalancer {

    long[][] getLocalLoad();

    long[] getTotalLoad();
}
