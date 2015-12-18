/**
 * 
 */
package statzall;

/**
 * Set of data science primitives, with associated properties. Quantiles will
 * have suffixes depending on the precision of the quantile, with min and max
 * being the bounds. For example, percentiles will have suffixes q000-q099.
 * 
 * @author mpouttuc
 *
 */
public enum Primitives {

	COUNT("count", true, false),
	COUNT_NULL("countNull", true, false),
	COUNT_NaN("countNaN", true, false),
	COUNT_USABLE("countUsable", true, false),
	COUNT_DISTINCT("countDistinct", false, false),
	MIN("min", true, true),
	MAX("max", true, true),
	MEDIAN("median", false, true),
	MEAN("mean", false, false),
	QUADRATIC_MEAN("quadraticMean", false, false),
	SUM("sum", true, false),
	SUM_OF_SQUARES("sumOfSquares", true, false),
	VARIANCE("variance", false, false),
	STANDARD_DEVIATION("standardDeviation", false, false),
	MEAN_ABS_DEVIATION("meanAbsDeviation", false, false),
	SKEWNESS("skewness", false, false),
	KURTOSIS("kurtosis", false, false),
	QUANTILE("q", false, true);

	public String alias;
	public boolean cumulative;
	public boolean quantile;

	private Primitives(String alias, boolean cumulative, boolean quantile) {
		this.alias = alias;
		this.cumulative = cumulative;
		this.quantile = quantile;
	}

}
