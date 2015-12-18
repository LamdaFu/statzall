package statzall.codec;

/**
 * Metadata for the 16 valid metaphone characters after the first, allowing
 * 4-bit encodings.
 */
public enum MD16 {
	DIGIT_0("0", '0', 0),
	ALPHA_B("bB", 'B', 1),
	ALPHA_F("fF", 'F', 2),
	ALPHA_H("hH", 'H', 3),
	ALPHA_J("jJ", 'J', 4),
	ALPHA_K("kK", 'K', 5),
	ALPHA_L("lL", 'L', 6),
	ALPHA_M("mM", 'M', 7),
	ALPHA_N("nN", 'N', 8),
	ALPHA_P("pP", 'P', 9),
	ALPHA_R("rR", 'R', 10),
	ALPHA_S("sS", 'S', 11),
	ALPHA_T("tT", 'T', 12),
	ALPHA_W("wW", 'W', 13),
	ALPHA_X("xX", 'X', 14),
	ALPHA_Y("yY", 'Y', 15);

	public String stringVal;
	public char charVal;
	public long bits;

	private MD16(String stringVal, char charVal, long bits) {
		this.stringVal = stringVal;
		this.charVal = charVal;
		this.bits = bits;
	}
}