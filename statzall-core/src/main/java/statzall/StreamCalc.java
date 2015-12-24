package statzall;

import static statzall.Primitives.COUNT;
import static statzall.Primitives.COUNT_DISTINCT;
import static statzall.Primitives.COUNT_NULL;
import static statzall.Primitives.COUNT_NaN;
import static statzall.Primitives.COUNT_USABLE;
import static statzall.Primitives.KURTOSIS;
import static statzall.Primitives.MAX;
import static statzall.Primitives.MEAN;
import static statzall.Primitives.MEAN_ABS_DEVIATION;
import static statzall.Primitives.MEDIAN;
import static statzall.Primitives.MIN;
import static statzall.Primitives.QUADRATIC_MEAN;
import static statzall.Primitives.QUANTILE;
import static statzall.Primitives.SKEWNESS;
import static statzall.Primitives.STANDARD_DEVIATION;
import static statzall.Primitives.SUM;
import static statzall.Primitives.SUM_OF_SQUARES;
import static statzall.Primitives.VARIANCE;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.math3.util.FastMath;

import cern.colt.buffer.DoubleBuffer;
import cern.colt.list.DoubleArrayList;
import cern.jet.random.engine.MersenneTwister;
import hep.aida.bin.QuantileBin1D;
import statzall.codec.Unibit;

public class StreamCalc implements Serializable {

	static final long serialVersionUID = -4240126532387563198L;
	long count, countNull, countNaN, countUsable, countDistinct;
	double madDelta;
	QuantileBin1D qb;
	DoubleBuffer queue;
	DoubleArrayList buff;
	String qFormat;
	DoubleArrayList phis;
	int queueDepth;
	Unibit unibit;

	StreamCalc() {
		super();
	}

	public StreamCalc(int quantiles, int queueDepth) {
		super();
		qb = new QuantileBin1D(false, Long.MAX_VALUE, 1.0e-5D, 1.0e-5D, quantiles,
				new MersenneTwister(Arrays.hashCode(new Object[] { Thread.currentThread(), System.nanoTime() })), false,
				false, 5);
		buff = new DoubleArrayList(queueDepth);
		queue = qb.buffered(queueDepth);
		qFormat = "%s%0" + (String.valueOf(quantiles).length() + 1) + "d";
		phis = calcPhi(quantiles);
		this.queueDepth = queueDepth;
		unibit = new Unibit();
		unibit.initCache();
	};

	public void add(Object... vals) {
		if (vals == null) {
			count++;
			countNull++;
		}
		for (Object val : vals) {
			if (String.class.isAssignableFrom(val.getClass())) {
				addOne(unibit.encode(String.valueOf(val)));
			} else if (Double.class.isAssignableFrom(val.getClass())) {
				addOne((Double) val);
			}
		}
	}

	public void addOne(Double val) {
		count++;
		if (val == null) {
			count++;
			countNull++;
			return;
		} else if (Double.isNaN(val)) {
			countNaN++;
			return;
		}
		buff.add(val);
		if (++countUsable % queueDepth == 0) {
			microBatch();
		}
	}

	public int microBatch() {
		int size = buff.size();
		queue.addAllOf(buff);
		queue.flush();
		madDelta += totalDelta(buff, qb.mean());
		buff.clear();
		return size;
	}

	double totalDelta(DoubleArrayList vals, double centroid) {
		double result = 0;
		for (int x = 0; x < vals.size(); x++) {
			result += FastMath.abs(vals.get(x) - centroid);
		}
		return result;
	}

	public SortedMap<String, Object> snapshot() {
		return snapshot(phis);
	}
	
	public SortedMap<String, Object> snapshot(int quantiles) {
		return snapshot(calcPhi(quantiles));
	}
	
	public SortedMap<String, Object> snapshot(double... phis) {
		return snapshot(new DoubleArrayList(phis));
	}
	
	SortedMap<String, Object> snapshot(DoubleArrayList phis) {
		microBatch();
		PatriciaTrie<Object> p = new PatriciaTrie<>();
		p.put(COUNT.alias, count);
		p.put(COUNT_NULL.alias, countNull);
		p.put(COUNT_NaN.alias, countNaN);
		p.put(COUNT_USABLE.alias, countUsable);
		// TODO: add distinct calc
		p.put(COUNT_DISTINCT.alias, countDistinct);
		p.put(MIN.alias, Unibit.decode(qb.min()));
		p.put(MAX.alias, Unibit.decode(qb.max()));
		p.put(MEDIAN.alias, Unibit.decode(qb.median()));
		p.put(MEAN.alias, qb.mean());
		p.put(QUADRATIC_MEAN.alias, qb.rms());
		p.put(SUM.alias, qb.sum());
		p.put(SUM_OF_SQUARES.alias, qb.sumOfSquares());
		p.put(VARIANCE.alias, qb.variance());
		p.put(STANDARD_DEVIATION.alias, qb.standardDeviation());
		p.put(MEAN_ABS_DEVIATION.alias, madDelta / countUsable);
		p.put(SKEWNESS.alias, qb.skew());
		p.put(KURTOSIS.alias, qb.kurtosis());
		DoubleArrayList vals = qb.quantiles(phis);
		for (int x = 0; x < vals.size(); x++) {
			p.put(String.format(qFormat, QUANTILE.alias, x), Unibit.decode(vals.get(x)));
		}
		// TODO: make NaN, Infinity, -Infinity configurable
		for (Map.Entry<String, Object> entry : p.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Double) {
				Double asDouble = Cast.as(value);
				if (asDouble == null || !Double.isFinite(asDouble)) {
					entry.setValue(String.valueOf(asDouble));
				}
			} else if (!(value instanceof Number)) {
				entry.setValue(String.valueOf(value));
			}
		}
		return Collections.unmodifiableSortedMap(new PatriciaTrie<>(p));
	}

	protected DoubleArrayList calcPhi(int quantiles) {
		DoubleArrayList phis = new DoubleArrayList();
		double incr = 1.0D / quantiles;
		for (int i = 1; i <= quantiles; i++) {
			phis.add(i * incr);
		}
		return phis;
	}

}
