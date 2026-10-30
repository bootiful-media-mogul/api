package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.utils.JsonUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * reads and writes the {@code edits} column, {@code {"kept": [[startMs, endMs], ...]}},
 * and renders the kept ranges in the form the processors read,
 * {@code 0-30000,60000-234960}.
 */
abstract class SegmentEdits {

	private static final String KEPT = "kept";

	static List<KeptRange> read(String json) {
		if (!StringUtils.hasText(json))
			return null;
		var edits = JsonUtils.read(json, new ParameterizedTypeReference<Map<String, List<List<Number>>>>() {
		});
		var kept = new ArrayList<KeptRange>();
		for (var range : edits.getOrDefault(KEPT, List.of()))
			kept.add(new KeptRange(range.get(0).longValue(), range.get(1).longValue()));
		return kept;
	}

	static String write(List<KeptRange> kept) {
		if (kept == null)
			return null;
		return JsonUtils.write(Map.of(KEPT, kept.stream().map(range -> List.of(range.start(), range.end())).toList()));
	}

	static String forProcessors(List<KeptRange> kept) {
		if (kept == null)
			return "";
		return kept.stream().map(range -> range.start() + "-" + range.end()).collect(Collectors.joining(","));
	}

}
