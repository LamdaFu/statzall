/**
 * 
 */
package statzall;

import java.util.Map;

/**
 * Utilities for strong typing in Map.get() and Objects, more robust than JDK
 * standard Class.cast()
 * 
 * @author mpouttuclarke
 *
 */
public class Cast {

	/**
	 * Attempt to get the key's value from the passed map, casting to the
	 * caller's desired type if possible.
	 * 
	 * If the key does not exist or the type is not as expected, returns a null.
	 * 
	 * @param map
	 * @param key
	 * @param returnType
	 * @return
	 */
	public static <T> T getAs(Map<? extends Object, ? extends Object> map, Object key, Class<T> returnType) {
		if (map == null || key == null || returnType == null) {
			return null;
		}
		Object object = map.get(key);
		return as(object, returnType);
	}

	/**
	 * Attempt to get the key's value from the passed map, casting to the
	 * caller's desired type if possible. Throws a ClassCastException in the
	 * case of a failed cast, and a NullPointerException if the map is null.
	 * 
	 * @param map
	 * @param key
	 * @return
	 * @throws NullPointerException
	 * @throws ClassCastException
	 */
	public static <T> T getAs(Map<? extends Object, ? extends Object> map, String key)
			throws NullPointerException, ClassCastException {
		return as(map.get(key));
	}

	/**
	 * Attempt to get the Objects value, casting to the caller's desired type if
	 * possible.
	 * 
	 * If the object does not exist or the type is not as expected, returns a
	 * null.
	 * 
	 * @param obj
	 * @param returnType
	 * @return
	 */
	@SuppressWarnings({ "unchecked" })
	public static <T> T as(Object object, Class<T> returnType) {
		if (object == null) {
			return null;
		} else if (!returnType.isAssignableFrom(object.getClass())) {
			if (returnType.isAssignableFrom(String.class)) {
				return (T)String.valueOf(object);
			}
			return null;
		} else {
			return (T) object;
		}
	}

	/**
	 * Attempt to cast the Object's value, casting to the caller's desired type
	 * if possible. Throws a ClassCastException in the case of a failed cast,
	 * and a NullPointerException if the object is null.
	 * 
	 * @param obj
	 * @return
	 * @throws NullPointerException
	 * @throws ClassCastException
	 */
	@SuppressWarnings({ "unchecked" })
	public static <T> T as(Object object) throws NullPointerException, ClassCastException {
		return (T) object;
	}
}
