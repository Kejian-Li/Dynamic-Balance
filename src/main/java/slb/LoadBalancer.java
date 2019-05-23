package slb;

public interface LoadBalancer {
	Server getSever(long timestamp, Object key);
}
