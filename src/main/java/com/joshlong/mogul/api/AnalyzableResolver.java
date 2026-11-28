package com.joshlong.mogul.api;

import java.util.List;

public interface AnalyzableResolver<T extends Analyzable> extends DomainResolver<Analyzable, T> {

	/**
	 * the id of the mogul who owns this analyzable, so that nobody can analyze someone
	 * else's things.
	 */
	Long mogulId(T analyzable);

	/**
	 * the text to analyze, in the parts a highlight can come from, in order: an episode's
	 * segments, as they play, or a post as a whole. parts with no text are left out, so
	 * an episode with no transcripts yet has none.
	 */
	List<AnalyzableSection> sections(T analyzable);

}
