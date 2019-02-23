package in.dreamlab.Euler;

/**
 * Utility class similar to Tuple2
 * 
 * @author simmhan
 *
 * @param <P>
 * @param <Q>
 */
public class TuplePair<P, Q> {
    private P _1;
    private Q _2;


    public TuplePair(P _1, Q _2) {
	this._1 = _1;
	this._2 = _2;
    }


    P _1() {
	return _1;
    }


    Q _2() {
	return _2;
    }


    @Override
    public boolean equals(Object o) {
	if (!(o instanceof TuplePair<?, ?>)) return false;
	TuplePair<?, ?> t = (TuplePair<?, ?>) o;
	// bug fixed in comparisons. Was earlier returning true incorrectly. Caused hashset to fail to add.
	return (_1 == t._1 || (_1 != null && _1.equals(t._1))) && (_2 == t._2 || (_2 != null && _2.equals(t._2))); 
    }


    @Override
    public String toString() {
	return "<" + _1 + "," + _2 + ">";
    }


    @Override
    public int hashCode() {
	return (_1.hashCode() ^ _2.hashCode());
    }
}
