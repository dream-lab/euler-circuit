package in.dream_lab.graph.euler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;


public class TroveBM {

    public static void main(String[] args) {
	// TODO Auto-generated method stub

	LinkedList<Long> obList; // remove
	LinkedList<Long> path; // add

	// OB List
	// traverse from end of array
	// do not remove after processing. reduce index location.
	// When deleted beyond a threshold, remove(int offset, int length) [removing data at end of array is cheap. no
	// array copy required] and then trimToSize(), which does array copy
	// Also, do not remove an OB sink vertex if added to path. Just set MSB flag.

	// addToLocalPath
	// getLocalNeighbourList
	// NOTE: removing end of array list is cheap. Just index reduction. Then trim periodically.

	Random rnd = new Random();

	int MAX = 10 * 1000 * 1000;
	long startTime, endTime, totTime;
	for (int size = 100; size < MAX; size *= 10) {
	    // init
	    TLongArrayList tList = new TLongArrayList(size);
	    LinkedList<Long> lList = new LinkedList<Long>();
	    ArrayList<Long> aList = new ArrayList<Long>(size);

	    long[] findItems = new long[size <= 100 ? size : 100];
	    int step = size <= 100 ? 1 : size / 100;
	    int j = 0;

	    // load
	    startTime = System.currentTimeMillis();
	    for (int i = 0; i < size; i++) {
		long l = rnd.nextLong();
		tList.add(l);
		lList.add(l);
		aList.add(l);
		if (i % step == 0) findItems[j++] = l;
	    }
	    endTime = System.currentTimeMillis();

	    System.out.printf("Add,Size,%d,TimeMS,%d%n", size, (endTime - startTime));

	    // find tlist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		int offset;
		startTime = System.nanoTime();
		offset = tList.indexOf(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (offset == -1) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Find-TList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);

	    // find alist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		int offset;
		startTime = System.nanoTime();
		offset = aList.indexOf(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (offset == -1) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Find-AList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);

	    // find llist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		int offset;
		startTime = System.nanoTime();
		offset = lList.indexOf(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (offset == -1) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Find-LList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);


	    // removed tlist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		boolean removed;
		startTime = System.nanoTime();
		removed = tList.remove(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (!removed) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Remove-TList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);

	    // removed alist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		boolean removed;
		startTime = System.nanoTime();
		removed = aList.remove(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (!removed) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Remove-AList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);

	    // removed llist
	    totTime = 0;
	    for (int i = 0; i < findItems.length; i++) {
		long x = findItems[i];
		boolean removed;
		startTime = System.nanoTime();
		removed = lList.remove(x);
		endTime = System.nanoTime();
		totTime += (endTime - startTime);
		if (!removed) System.out.printf("Could not find %d in index %d of findItems for tlist%n", x, i);
	    }

	    System.out.printf("Remove-LList,Size,%d,AvgTimeNS,%d%n", findItems.length, totTime / findItems.length);


	}

    }

}
