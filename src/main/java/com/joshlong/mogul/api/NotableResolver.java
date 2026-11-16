package com.joshlong.mogul.api;

public interface NotableResolver<T extends Notable> extends DomainResolver<Notable, T> {

	/**
	 * the id of the mogul who owns this notable, so that nobody can attach a note to
	 * someone else's things.
	 */
	Long mogulId(T notable);

}
