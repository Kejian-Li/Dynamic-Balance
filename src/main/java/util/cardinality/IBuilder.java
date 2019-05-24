package util.cardinality;


public interface IBuilder<T>
{
    T build();

    int sizeof();
}
