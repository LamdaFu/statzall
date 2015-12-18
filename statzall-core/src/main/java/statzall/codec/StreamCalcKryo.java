package statzall.codec;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.objenesis.strategy.SerializingInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import statzall.StreamCalc;

/**
 * Isolate the Kryo dependency and related logic.
 * 
 * @author mpouttuclarke
 *
 */
public class StreamCalcKryo {
	private static final Cache<Thread, KryoVals> BUFF_CACHE;

	static class KryoVals {
		final Kryo kryo;
		final Serializer<?> ser;
		final byte[] buff;

		protected KryoVals(Kryo kryo, Serializer<?> ser, byte[] buff) {
			super();
			this.kryo = kryo;
			this.ser = ser;
			this.buff = buff;
		}
	}

	static {
		BUFF_CACHE = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.SECONDS).build();
	}

	public final int buffSize;

	public StreamCalcKryo() {
		super();
		buffSize = 1024 * 1024 * 128;
	}

	public StreamCalcKryo(int buffSize) {
		super();
		this.buffSize = buffSize;
	}

	public StreamCalc read(byte[] inBuff) {
		KryoVals vals = getKryoVals();
		return vals.kryo.readObject(new Input(inBuff), StreamCalc.class, vals.ser);
	}

	public byte[] write(StreamCalc obj) {
		KryoVals vals = getKryoVals();
		Output out = new Output(vals.buff);
		vals.kryo.writeObject(out, obj, vals.ser);
		return ArrayUtils.subarray(vals.buff, 0, out.position());
	}

	KryoVals getKryoVals() {
		try {
			return BUFF_CACHE.get(Thread.currentThread(), new Callable<KryoVals>() {
				@Override
				public KryoVals call() throws Exception {
					Kryo kryo = new Kryo();
					kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
					return new KryoVals(kryo,
							kryo.register(StreamCalc.class, new FieldSerializer<>(kryo, StreamCalc.class), 16)
									.getSerializer(),
							new byte[buffSize]);
				}
			});
		} catch (ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}

}
