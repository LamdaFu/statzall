package statzall;

import static org.junit.Assert.*;

import org.junit.Test;

import cern.jet.random.Normal;
import cern.jet.random.engine.MersenneTwister;
import statzall.StreamCalc;

public class StreamCalcTest {

	@Test
	public void test() {
		StreamCalc target = new StreamCalc(12, 1024);
		target.add("jimbo", "jim", "bob", "zeke");
		System.out.println(target.snapshot());
		target = new StreamCalc(10, 1024);
		Normal norm = new Normal(10000, 100, new MersenneTwister(Thread.currentThread().hashCode()));
		long start = System.currentTimeMillis();
		int count = 20000000;
		for(int x = 0; x < count; x++) {
			target.addOne((double)norm.nextInt());
		}
		printRate(start, count, "Number only calc");
		target.snapshot();
		printRate(start, count, "Quantiles");
		start = System.currentTimeMillis();
		count = 5000000;
		for(int x = 0; x < count; x++) {
			target.add(String.valueOf(norm.nextInt()));
		}
		printRate(start, count, "Number as String calc");
		System.out.println(target.snapshot());
	}

	private void printRate(long start, int count, String measure) {
		double secs = (System.currentTimeMillis() - start) / 1000D;
		System.out.println(String.format("%s count: %s, secs: %.3f, count/sec: %.0f", measure, count, secs, count / secs));
	}

}
