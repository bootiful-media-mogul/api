package com.joshlong.mogul.api.notes;

import java.net.URI;
import java.util.Locale;
import java.util.Set;

/**
 * a note's url is clipped from the web and rendered as a link in the studio, so it has to
 * be an absolute http(s) url: anything else ({@code javascript:}, {@code file:}, a
 * relative path) is either useless or dangerous to click.
 */
abstract class NoteUrls {

	private static final Set<String> SCHEMES = Set.of("http", "https");

	static void assertWebUrl(URI url) {
		if (url == null)
			return;
		var scheme = url.getScheme() == null ? null : url.getScheme().toLowerCase(Locale.ROOT);
		if (!url.isAbsolute() || !SCHEMES.contains(scheme) || url.getHost() == null)
			throw new IllegalArgumentException("a note's url must be an absolute http or https url: [" + url + "]");
	}

	static URI parse(String url) {
		if (url == null || url.isBlank())
			return null;
		try {
			var uri = new URI(url.strip());
			assertWebUrl(uri);
			return uri;
		}
		catch (java.net.URISyntaxException e) {
			throw new IllegalArgumentException("a note's url must be an absolute http or https url: [" + url + "]", e);
		}
	}

}
