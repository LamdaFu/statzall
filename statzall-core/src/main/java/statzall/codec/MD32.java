package statzall.codec;

/**
 * Metadata for valid metaphone first characters, allowing 5-bit encoding of the
 * first character in a metaphone string.
 * 
 * @author mpouttuclarke
 *
 */
public enum MD32 {
	ALPHA_A("aA", 'A', 0),
	ALPHA_B("bB", 'B', 1),
	ALPHA_C("cC", 'C', 2),
	ALPHA_D("dD", 'D', 3),
	ALPHA_E("eE", 'E', 4),
	ALPHA_F("fF", 'F', 5),
	ALPHA_G("gG", 'G', 6),
	ALPHA_H("hH", 'H', 7),
	ALPHA_I("iI", 'I', 8),
	ALPHA_J("jJ", 'J', 9),
	ALPHA_K("kK", 'K', 10),
	ALPHA_L("lL", 'L', 11),
	ALPHA_M("mM", 'M', 12),
	ALPHA_N("nN", 'N', 13),
	ALPHA_O("oO", 'O', 14),
	ALPHA_P("pP", 'P', 15),
	ALPHA_Q("qQ", 'Q', 16),
	ALPHA_R("rR", 'R', 17),
	ALPHA_S("sS", 'S', 18),
	ALPHA_T("tT", 'T', 19),
	ALPHA_U("uU", 'U', 20),
	ALPHA_V("vV", 'V', 21),
	ALPHA_W("wW", 'W', 22),
	ALPHA_X("xX", 'X', 23),
	ALPHA_Y("yY", 'Y', 24),
	ALPHA_Z("zZ", 'Z', 25);

	public String stringVal;
	public char charVal;
	public long bits;

	private MD32(String stringVal, char charVal, long bits) {
		this.stringVal = stringVal;
		this.charVal = charVal;
		this.bits = bits;
	}
}