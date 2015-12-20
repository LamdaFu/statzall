/**
 * 
 */
package statzall.cdap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.base.Charsets;

import cern.jet.random.Normal;
import cern.jet.random.engine.MersenneTwister;

/**
 * @author mpouttuc
 *
 */
public class TestDataGen {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		long DEFAULT_RECORDS = 1000000L;
		long count = (args == null || args.length < 1 ? DEFAULT_RECORDS : NumberUtils.toLong(args[0], DEFAULT_RECORDS));
		Normal norm = new Normal(10000, 100, new MersenneTwister(Thread.currentThread().hashCode()));
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out, Charsets.UTF_8), 32768);
		for(long x = 0; x < count; x++) {
			out.write(String.format("key%s,%s\n", x % 4, norm.nextInt()));
		}
		out.close();
	}

}
