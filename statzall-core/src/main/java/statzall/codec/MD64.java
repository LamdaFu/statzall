package statzall.codec;

/**
 * Metadata for a subset of 64 meaningful ASCII characters, allowing 6-bit
 * encoding.
 * 
 * @author mpouttuclarke
 *
 */
public enum MD64 {
	NULL("\0", '\0', 0),
	TAB("\t", '\t', 1),
	NEWLINE("\n", '\n', 2),
	SPACE(" ", ' ', 3),
	EXPLANATION("!", '!', 4),
	POUND("#", '#', 5),
	DOLLAR("$", '$', 6),
	PERCENT("%", '%', 7),
	AND("&", '&', 8),
	LPAREN("(", '(', 9),
	RPAREN(")", ')', 10),
	ASTERIC("*", '*', 11),
	PLUS("+", '+', 12),
	COMMA(",", ',', 13),
	DASH("-", '-', 14),
	DOT(".", '.', 15),
	SLASH("/", '/', 16),
	DIGIT_0("0", '0', 17),
	DIGIT_1("1", '1', 18),
	DIGIT_2("2", '2', 19),
	DIGIT_3("3", '3', 20),
	DIGIT_4("4", '4', 21),
	DIGIT_5("5", '5', 22),
	DIGIT_6("6", '6', 23),
	DIGIT_7("7", '7', 24),
	DIGIT_8("8", '8', 25),
	DIGIT_9("9", '9', 26),
	COLON(":", ':', 27),
	GT("<", '<', 28),
	EQUAL("=", '=', 29),
	LT(">", '>', 30),
	QUESTION("?", '?', 31),
	AT("@", '@', 32),
	ALPHA_A("aA", 'A', 33),
	ALPHA_B("bB", 'B', 34),
	ALPHA_C("cC", 'C', 35),
	ALPHA_D("dD", 'D', 36),
	ALPHA_E("eE", 'E', 37),
	ALPHA_F("fF", 'F', 38),
	ALPHA_G("gG", 'G', 39),
	ALPHA_H("hH", 'H', 40),
	ALPHA_I("iI", 'I', 41),
	ALPHA_J("jJ", 'J', 42),
	ALPHA_K("kK", 'K', 43),
	ALPHA_L("lL", 'L', 44),
	ALPHA_M("mM", 'M', 45),
	ALPHA_N("nN", 'N', 46),
	ALPHA_O("oO", 'O', 47),
	ALPHA_P("pP", 'P', 48),
	ALPHA_Q("qQ", 'Q', 49),
	ALPHA_R("rR", 'R', 50),
	ALPHA_S("sS", 'S', 51),
	ALPHA_T("tT", 'T', 52),
	ALPHA_U("uU", 'U', 53),
	ALPHA_V("vV", 'V', 54),
	ALPHA_W("wW", 'W', 55),
	ALPHA_X("xX", 'X', 56),
	ALPHA_Y("yY", 'Y', 57),
	ALPHA_Z("zZ", 'Z', 58),
	LBRACKET("[", '[', 59),
	BACKSLASH("\\", '\\', 60),
	RBRACKET("]", ']', 61),
	UPTICK("^", '^', 62),
	UNDERSCORE("_", '_', 63);

	public String stringVal;
	public char charVal;
	public long bits;

	private MD64(String stringVal, char charVal, long bits) {
		this.stringVal = stringVal;
		this.charVal = charVal;
		this.bits = bits;
	}

}