package statzall.codec;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Packs an ASCII compatible String into a double while preserving as much
 * information as possible. If the String represents a Number, then it is
 * encoded as such. If not, the PM64 enum defines a 6-bit encoding scheme used
 * for values up to literalCutoff characters. After that length, the input
 * String is phonetically encoded and then the PM16 enum is used for a 4-bit
 * encoding of the phonetic characters.
 * 
 * The packed value contains reserved bits so that more complex phonetic
 * encodings (including international charsets) can be added.
 * 
 * Also supports utility methods to identify the encoded data type and also to
 * decode back to the original String (if literalCutoff or less chars) or the
 * phonetic string (if literalCutoff+ chars).
 * 
 * Numeric input less than or equal to -281474976710656L will be treated as
 * negative infinity, in order to reserve flags to allow accurate decoding of
 * String input.
 * 
 * If encoding errors occur or any reason, a NaN result occurs.
 * 
 * @author mpouttuclarke
 *
 */
// TODO: add special date/time 4-bit encoding
public class Unibit implements Serializable {

	private static final long serialVersionUID = 6285670160842403438L;
	private static final MD16[] MD16_VALUES = MD16.values();
	private static final MD64[] MD64_VALUES = MD64.values();

	public static final int LITERAL_CUTOFF_MAX = 8;
	public static final int LITERAL_CUTOFF_DEFAULT = 4;

	private static final String NUMERICS = "0123456789";

	public static final long ENCODED_IND_BITS = 0x10000000000000L;
	public static final long MAX_SEGMENT_BITS = 0x00FFFFFFFFFFFFL;
	public static final long ENCODED_MAX_BITS = 0x1FFFFFFFFFFFFFL;
	public static final double ENCODED_MAX = Double.longBitsToDouble(-ENCODED_MAX_BITS);
	public static final double ENCODED_MD64_MAX = Double.longBitsToDouble(-(ENCODED_IND_BITS | MAX_SEGMENT_BITS));
	public static final long ENCODED_MD16_BITS = 0x11000000000000L;
	public static final double ENCODED_MD16_MIN = Double.longBitsToDouble(-ENCODED_MD16_BITS);
	public static final double ENCODED_MD16_MAX = Double.longBitsToDouble(-(ENCODED_MD16_BITS | MAX_SEGMENT_BITS));

	private static final PatriciaTrie<MD64> MD64_TRIE = new PatriciaTrie<>();
	private static final PatriciaTrie<MD32> MD32_TRIE = new PatriciaTrie<>();
	private static final PatriciaTrie<MD16> MD16_TRIE = new PatriciaTrie<>();
	private static final DoubleMetaphone DM = new DoubleMetaphone();

	static {
		for (MD64 validChars : MD64.values()) {
			String stringVal = validChars.stringVal;
			for (int x = 0; x < stringVal.length(); x++) {
				String validChar = stringVal.substring(x, x + 1);
				MD64_TRIE.put(validChar, validChars);
			}
		}
		for (MD32 validChars : MD32.values()) {
			String stringVal = validChars.stringVal;
			for (int x = 0; x < stringVal.length(); x++) {
				String validChar = stringVal.substring(x, x + 1);
				MD32_TRIE.put(validChar, validChars);
			}
		}
		for (MD16 validChars : MD16.values()) {
			String stringVal = validChars.stringVal;
			for (int x = 0; x < stringVal.length(); x++) {
				String validChar = stringVal.substring(x, x + 1);
				MD16_TRIE.put(validChar, validChars);
			}
		}
		DM.setMaxCodeLen(12);
	}

	private int literalCutoff = LITERAL_CUTOFF_DEFAULT;
	private int maxCacheSize = 64;
	private transient Cache<String, Double> cache;

	public Unibit() {
		super();
	}

	/**
	 * Initializes or resets the operation cache.
	 */
	public void initCache() {
		cache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build();
	}

	/**
	 * String length cutoff after which phonetic encoding replaces MD64
	 * encoding. For example, if the data is acronyms or other already encoded
	 * values then phonetic re-encoding doesn't make sense and can be turned off
	 * by setting literalCutoff to 8. If the value is longer than 8 characters
	 * then phonetic encoding will be used regardless.
	 * 
	 * @param literalCutoff
	 * @return
	 */
	public Unibit withLiteralCutoff(int literalCutoff) {
		if (literalCutoff < 0 || literalCutoff > LITERAL_CUTOFF_MAX) {
			this.literalCutoff = LITERAL_CUTOFF_DEFAULT;
		} else {
			this.literalCutoff = literalCutoff;
		}
		return this;
	}

	/**
	 * Operation cache size which saves operation results for common inputs.
	 * 
	 * @param maxCacheSize
	 * @return
	 */
	public Unibit withMaxCacheSize(int maxCacheSize) {
		this.maxCacheSize = maxCacheSize;
		return this;
	}

	public Double encode(final String value) {
		if (StringUtils.isEmpty(value)) {
			return null;
		}
		try {
			if(cache == null) {
				initCache();
			}
			return cache.get(value, new Callable<Double>() {
				@Override
				public Double call() throws Exception {
					return encodeInternal(value);
				}
			});
		} catch (ExecutionException e) {
			return Double.NaN;
		}
	}

	protected double encodeInternal(final String value) {
		if (StringUtils.isEmpty(value)) {
			return Double.NaN;
		}
		if (NumberUtils.isNumber(value)) {
			return encodeRawDouble(value);
		}
		String[] subStrings = subStrings(value);
		if (StringUtils.containsAny(value, NUMERICS) && containsNoMD32(subStrings)) {
			// If no characters, strip out any numbers and convert
			StringBuilder stripNonNumeric = new StringBuilder();
			for (int x = 0; x < subStrings.length; x++) {
				char charAt = subStrings[x].charAt(0);
				if (charAt >= '0' && charAt <= '9') {
					stripNonNumeric.append(charAt);
				}
			}
			return encodeRawDouble(stripNonNumeric.toString());
		}
		if (value.length() <= literalCutoff) {
			return encodeMD64(subStrings);
		} else {
			return encodeDoubleMetaphone(value);
		}
	}

	protected double encodeRawDouble(final String value) {
		double encoded = NumberUtils.toDouble(value, Double.NaN);
		if (encoded <= ENCODED_MAX) { // prevent occlusion on encoded space
			encoded = Double.NEGATIVE_INFINITY;
		}
		return encoded;
	}

	/*
	 * TODO: we should really commit a change to commons collections4 to avoid
	 * splitting the string (support char[] or byte[] in PatriciaTrie)
	 */
	private static String[] subStrings(final String value) {
		final String[] subStrings = new String[value.length()];
		for (int x = 0; x < value.length(); x++) {
			subStrings[x] = value.substring(x, x + 1);
		}
		return subStrings;
	}

	private static boolean containsNoMD32(String... values) {
		for (int x = 0; x < values.length; x++) {
			if (MD32_TRIE.containsKey(values[x])) {
				return false;
			}
		}
		return true;
	}

	private static double encodeMD64(String[] subStrings) {
		long bits = 0L;
		for (int x = 0; x < subStrings.length; x++) {
			MD64 md64 = MD64_TRIE.get(subStrings[x]);
			if (md64 == null) {
				continue;
			}
			long xform = md64.bits;
			if (x > 0) {
				bits <<= 6;
			}
			bits |= xform;
		}
		bits |= ENCODED_IND_BITS;
		return Double.longBitsToDouble(-bits);
	}

	private static double encodeDoubleMetaphone(final String value) {
		String dm = DM.doubleMetaphone(value);
		long bits = 0L;
		for (int x = 0; x < dm.length(); x++) {
			MD16 md16 = MD16_TRIE.get(dm.substring(x, x + 1));
			if (md16 == null) {
				continue;
			}
			if (x > 0) {
				bits <<= 4;
			}
			bits |= md16.bits;
		}
		bits |= ENCODED_MD16_BITS;
		return Double.longBitsToDouble(-bits);
	}

	/**
	 * Output as a simple Double, or decode the String if the number represents
	 * an encoded String.
	 * 
	 * @param d
	 * @return
	 */
	public static Object decode(double d) {
		long bits = -Double.doubleToRawLongBits(d);
		if (isRawNumber(d)) {
			return d;
		}
		StringBuilder result = new StringBuilder(15);
		long bitmask = 0x3F; // 6-bit
		if (isMD64(d)) {
			for (int x = 6 * 7; x > -1; x -= 6) {
				long shift = bits >> x;
				int i = (int) (shift & bitmask);
				if (i > 0 && i < MD64_VALUES.length) {
					result.append(MD64_VALUES[i].charVal);
				}
			}
		} else if (d <= ENCODED_MD16_MAX) {
			bitmask /= 4; // Switch to 4-bit
			for (int x = 4 * 11; x > -1; x -= 4) {
				long shift = bits >> x;
				int i = (int) (shift & bitmask);
				if (i > 0 && i < MD16_VALUES.length) {
					result.append(MD16_VALUES[i].charVal);
				}
			}
		}
		return result.toString();
	}
	
	public static boolean isRawNumber(double d) {
		return d > ENCODED_MAX;
	}

	public static boolean isMD16(double d) {
		return d <= ENCODED_MD16_MAX && d >= ENCODED_MD16_MIN;
	}
	
	protected static boolean isMD64(double d) {
		return d <= ENCODED_MD64_MAX;
	}
}
