package com.joshlong.mogul.api.compositions;

/**
 * says which mogul owns the thing a {@link Composition} belongs to, so that the
 * compositions api can refuse anyone else. a composition has no owner of its own: it
 * hangs off a {@link Composable} (an episode, a post, ...) by class and key, and the
 * domain that owns that class knows whose it is.
 */
public interface ComposableOwnerResolver {

	boolean supports(Class<?> composableClass);

	/**
	 * the id of the mogul who owns the composable of the given class and key, or
	 * {@code null} if there's no such thing.
	 */
	Long mogulId(Class<?> composableClass, Long compositionKey);

}
