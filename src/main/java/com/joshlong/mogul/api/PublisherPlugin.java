package com.joshlong.mogul.api;

import org.jspecify.annotations.NonNull;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public interface PublisherPlugin<T extends Publishable> {

	String name();

	default Set<PublisherSetting> pluginSettings() {
		return Set.of();
	}

	default boolean isConfigurationValid(Map<String, String> context) {
		var required = this.pluginSettings()
			.stream()
			.filter(PublisherSetting::required)
			.map(PublisherSetting::name)
			.toList();

		if (context == null)
			context = new HashMap<>();

		var good = true;
		for (var k : required) {
			// the value has to be worth something, not merely be present. clearing a
			// required setting on the settings page leaves the row in place with an
			// empty value, so containsKey() went on reporting the plugin as configured
			// and the publish button stayed lit for something that could not work.
			// this is the same test the settings page applies to each field.
			if (!StringUtils.hasText(context.get(k))) {
				good = false;
				break;
			}
		}
		return good;
	}

	default boolean canPublish(PublishContext<T> publishContext) {
		return isConfigurationValid(publishContext.context()) && publishContext.payload() != null;
	}

	/**
	 * whether a publication through this plugin may be asked for at a later time. most
	 * can: every publication runs on jobrunr, now or later, the same way. the ones that
	 * can't are those whose result the user is waiting on right now, like a download.
	 * clients read this (through the {@code publisherPlugins} query) to decide whether to
	 * offer scheduling, and the publication service refuses a schedule for a plugin that
	 * says no.
	 */
	default boolean schedulable() {
		return true;
	}

	void publish(PublishContext<T> publishContext);

	boolean unpublish(UnpublishContext<T> uc);

	record PublisherSetting(boolean required, String name) {
	}

	class Context<T> {

		private final Map<String, String> context;

		Context(Map<String, String> inputContext) {
			this.context = null == inputContext ? new ConcurrentHashMap<>() : inputContext;
		}

		public Map<String, String> context() {
			return this.context;
		}

	}

	class UnpublishContext<T> extends Context<T> {

		private final Publication publication;

		public UnpublishContext(Map<String, String> inputContext, Publication publication) {
			super(inputContext);
			this.publication = publication;
		}

		public Publication publication() {
			return this.publication;
		}

	}

	class PublishContext<T> extends Context<T> {

		/**
		 * how much of a preview survives. an outcome row in the UI is one line beside an
		 * icon and a link; this is about as much as fits there without wrapping.
		 */
		public static final int PREVIEW_LENGTH = 100;

		private final T payload;

		private final Long mogulId;

		private final List<Outcome> outcomes = new ArrayList<>();

		PublishContext(Long mogulId, T payload, Map<String, String> inputContext) {
			super(inputContext);
			this.payload = payload;
			this.mogulId = mogulId;
		}

		public static <T> PublishContext<T> of(Long mogulId, T payload, Map<String, String> c) {
			return new PublishContext<>(mogulId, payload, c);
		}

		/**
		 * one line, short enough that the UI doesn't have to wrap it. newlines and runs
		 * of whitespace collapse to single spaces -- a tweet's paragraph breaks would
		 * otherwise arrive as a preview that's mostly blank -- and anything longer than
		 * {@link #PREVIEW_LENGTH} is cut at the last word boundary and given an ellipsis.
		 * done here, once, so every plugin's previews look alike and the column can't be
		 * blown out by one of them.
		 */
		static String preview(String preview) {
			if (!StringUtils.hasText(preview))
				return null;
			var oneLine = preview.replaceAll("\\s+", " ").trim();
			if (oneLine.length() <= PREVIEW_LENGTH)
				return oneLine;
			var cut = oneLine.substring(0, PREVIEW_LENGTH);
			var lastSpace = cut.lastIndexOf(' ');
			// no space to break on means one very long word (a URL, usually); cutting it
			// mid-word beats returning the whole thing.
			if (lastSpace > PREVIEW_LENGTH / 2)
				cut = cut.substring(0, lastSpace);
			return cut.stripTrailing() + "\u2026";
		}

		public Long mogulId() {
			return this.mogulId;
		}

		public T payload() {
			return this.payload;
		}

		public PublishContext<T> success(String outcomeKey, URI outcome) {
			return this.outcome(outcomeKey, true, outcome, null, null);
		}

		/**
		 * for the plugins whose outcome is worth reading and not just following: the
		 * social posts, where one publish sends different text to each platform and the
		 * link alone doesn't say which post is which. pass the whole thing; it's
		 * abbreviated to one short line on the way in, so nothing here has to know how
		 * much of it the UI can fit.
		 */
		public PublishContext<T> success(String outcomeKey, URI outcome, String preview) {
			return this.outcome(outcomeKey, true, outcome, null, preview);
		}

		public PublishContext<T> failure(String outcomeKey, String errorMessageFromServer) {
			return this.outcome(outcomeKey, false, null, errorMessageFromServer, null);
		}

		public PublishContext<T> failure(String outcomeKey, String errorMessageFromServer, String preview) {
			return this.outcome(outcomeKey, false, null, errorMessageFromServer, preview);
		}

		private PublishContext<T> outcome(String outcomeKey, boolean success, URI outcome, String errorMessage,
				String preview) {
			this.outcomes.add(new Outcome(outcome, outcomeKey, success, errorMessage, preview(preview)));
			return this;
		}

		@NonNull
		public List<Outcome> outcomes() {
			return this.outcomes;
		}

		public record Outcome(URI uri, String key, boolean success, String serverErrorMessage, String preview) {
		}

	}

}
