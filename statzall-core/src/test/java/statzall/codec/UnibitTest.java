package statzall.codec;

import static org.junit.Assert.*;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.junit.Test;

public class UnibitTest {

	@Test
	public void test() {
		Unibit target = new Unibit().withLiteralCutoff(8);
		double justADouble = target.encode("-1");
		assertEquals(-1.0D, justADouble, 0.0D);
		assertTrue(Unibit.isRawNumber(justADouble) && !Unibit.isMD64(justADouble)
				&& !Unibit.isMD16(justADouble));
		assertEquals(1.0D, target.encode("\t\t\t\t\t\t\t\t1"), 0.0D);
		assertEquals(0.1D, target.encode("1.0E-1"), 0.0D);
		String md64expected = "___F____";
		double md64 = target.encode(md64expected);
		assertTrue(!Unibit.isRawNumber(md64) && Unibit.isMD64(md64) && !Unibit.isMD16(md64));
		assertEquals(-1.7415153081094787E308, md64, 0.0D);
		assertEquals(md64expected, Unibit.decode(md64));
		String phoneticExpected = "bilbo baggins of bag end and barrelrider";
		double phonetic = target.encode(phoneticExpected);
		assertEquals(-1.7084670839062802E308, phonetic, 0.0D);
		assertTrue(!Unibit.isRawNumber(phonetic) && !Unibit.isMD64(phonetic)
				&& Unibit.isMD16(phonetic));
		DoubleMetaphone dm = new DoubleMetaphone();
		dm.setMaxCodeLen(12);
		assertEquals(dm.doubleMetaphone(phoneticExpected), Unibit.decode(phonetic));
	}

}
