package com.joshlong.mogul.api;

/**
 * one part of an {@link Analyzable}'s text, that a highlight can say it came from: one of
 * an episode's segments, say, or the whole of a post.
 *
 * @param key what the part is known by, unique among the analyzable's parts: a segment's
 * id
 * @param name what the part is called, or null if it has no name of its own
 * @param text the part's text
 */
public record AnalyzableSection(String key, String name, String text) {
}
