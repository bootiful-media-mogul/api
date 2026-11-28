package com.joshlong.mogul.api;

import java.util.Date;
import java.util.List;

/**
 * what the model found most compelling in an {@link Analyzable}'s text. there's one per
 * analyzable: analyzing it again replaces it.
 *
 * @param payload the analyzable's key, as json
 * @param payloadClass the analyzable's class
 * @param highlights the compelling parts, best first
 */
public record Analysis(Long mogulId, Long id, Date created, String payload, Class<?> payloadClass,
		List<Highlight> highlights) {

	/**
	 * one compelling part of the text.
	 *
	 * @param kind why it's compelling: {@code entertaining}, {@code useful}, or
	 * {@code quotable}
	 * @param quote the part, word for word as the text has it
	 * @param reason why it's compelling, in a sentence
	 * @param score how compelling, from 1 to 10
	 * @param section the key of the {@link AnalyzableSection} the quote is in: a
	 * segment's id
	 * @param sectionName what that section was called when it was analyzed, or null
	 */
	public record Highlight(String kind, String quote, String reason, int score, String section, String sectionName) {
	}

}
