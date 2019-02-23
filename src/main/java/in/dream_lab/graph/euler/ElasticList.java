package in.dream_lab.graph.euler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;


/**
 * Expandable list constructed from a list of arrays.
 * Supports only add and iterate operations.
 * Provides locality of arrays with expandability of arraylist without memory copies.
 * 
 * @author simmhan
 *
 * @param <E>
 */
public class ElasticList<E> implements Iterable<E> {

    private int bucketSize;
    private List<E[]> buckets;
    private Function<Integer, E[]> factory;

    private int lastBucketIndex, lastIndex;
    private E[] lastBucket;


    public ElasticList(Function<Integer, E[]> factory) {
	this(factory, 1000, 10);
    }


    public ElasticList(Function<Integer, E[]> factory, int bucketSize, int bucketCount) {
	this.factory = factory;
	this.bucketSize = bucketSize;
	assert bucketSize > 0 && bucketCount > 0;
	buckets = new ArrayList<>(bucketCount);
	lastBucketIndex = -1;
	addBucket();
    }


    protected void addBucket() {
	lastBucket = factory.apply(bucketSize);
	buckets.add(lastBucket);
	lastBucketIndex++;
	lastIndex = -1;
    }


    public void add(E item) {
	lastIndex++;
	if (lastIndex == bucketSize) { // add new unit
	    addBucket();
	    lastIndex++;
	}
	lastBucket[lastIndex] = item;
    }

    // Code works. But this is 10x slower than user defined iterator and operation. So dropping support.
    //
    // <R> R aggregate(Function<E, R> func1, BiFunction<R, R, R> func2, R init) {
    // int localBucketIndex = 0;
    // int localIndex = -1;
    // R out = init;
    // while ((localBucketIndex < lastBucketIndex)
    // || (localBucketIndex == lastBucketIndex && localIndex < lastIndex)) {
    // localIndex++;
    // if (localIndex == bucketSize) {
    // localBucketIndex++;
    // localIndex = 0;
    // }
    // E[] bucket = buckets.get(localBucketIndex);
    // R r = func1.apply(bucket[localIndex]);
    // out = func2.apply(out, r);
    // }
    // return out;
    // }


    public long size() {
	return (long) lastBucketIndex * (long) bucketSize + (long) lastIndex + 1;
    }

//    public boolean isEmpty() {
//    	// TODO
//    }
//    
//    public E peek() {
//    	// TODO: return head if not empty
//    }
//    
//    public E remove() {
//    	// TODO: removes and returns the head if not empty. Removal is done by setting the base index to point to after this item. If this is the last index of the first bucket, the bucket is removed.
//    }
//
//    public E poll() {
//    	// TODO: returns and removes the head
//    }
    

    @Override
    public Iterator<E> iterator() {
	return new ElasticListIter();
    }


    class ElasticListIter implements Iterator<E> {

	int localBucketIndex, localIndex;


	protected ElasticListIter() {
	    localBucketIndex = 0;
	    localIndex = -1;
	}


	@Override
	public boolean hasNext() {
	    return (localBucketIndex < lastBucketIndex)
		    || (localBucketIndex == lastBucketIndex && localIndex < lastIndex);
	}


	@Override
	public E next() {
	    localIndex++;
	    if (localIndex == bucketSize) { // add new unit
		localBucketIndex++;
		localIndex = 0;
	    }
	    E[] bucket = buckets.get(localBucketIndex);
	    return bucket[localIndex];
	}
    }


    public static void main(String[] args) {

	ElasticList<int[]> list1 = new ElasticList<>(n -> new int[n][], 1000, 100);
	List<int[]> list2 = new ArrayList<>();

	// generate data
	int n = 10000000;
	int m = 2;
	Random rnd = new Random();

	for (int i = 0; i < n; i++) {
	    int x[] = new int[m];
	    for (int j = 0; j < m; j++) {
		x[j] = rnd.nextInt(100);
	    }
	    list1.add(x);
	    list2.add(x);
	}

	// perf test: iter vs. aggregate
	// RESULT: agg is 10x slower than iter!!!
	// long start = System.currentTimeMillis();
	// int count = 0;
	// for (int[] y : list1) {
	// count += y.length;
	// }
	// long end = System.currentTimeMillis();
	// System.out.printf("iter: %d, count: %d%n", (end - start), count);
	//
	//
	// start = System.currentTimeMillis();
	// count = list1.aggregate(v -> v.length, (x, y) -> x + y, 0);
	// end = System.currentTimeMillis();
	// System.out.printf("aggr: %d, count: %d%n", (end - start), count);


	// // Comparison test
	// for (int[] y : list1) {
	// for (int j = 0; j < y.length; j++) {
	// System.out.print(y[j] + ",");
	// }
	// System.out.println();
	// }
	//
	// System.out.println();
	//
	// for (int[] y : list2) {
	// for (int j = 0; j < y.length; j++) {
	// System.out.print(y[j] + ",");
	// }
	// System.out.println();
	// }

    }
}

