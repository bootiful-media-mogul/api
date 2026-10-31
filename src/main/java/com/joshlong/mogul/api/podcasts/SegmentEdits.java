package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.utils.JsonUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * reads and writes the {@code edits} column: {@code {"kept": [[startMs, endMs], ...],
 * "filters": {...}, "restored": [...]}}, every part optional. renders the kept ranges in
 * the form the processors read, {@code 0-30000,60000-234960}.
 */
abstract class SegmentEdits {

	private static final String KEPT = "kept";

	private static final String FILTERS = "filters";

	private static final String RESTORED = "restored";

	private static Map<String, Object> parse(String json) {
		if (!StringUtils.hasText(json))
			return null;
		return JsonUtils.read(json, new ParameterizedTypeReference<Map<String, Object>>() {
		});
	}

	/** the ranges kept by hand, or null if there aren't any */
	static List<KeptRange> read(String json) {
		var edits = parse(json);
		if (edits == null || !(edits.get(KEPT) instanceof List<?> ranges))
			return null;
		var kept = new ArrayList<KeptRange>();
		for (var range : ranges)
			if (range instanceof List<?> pair)
				kept.add(new KeptRange(((Number) pair.get(0)).longValue(), ((Number) pair.get(1)).longValue()));
		return kept;
	}

	/** the automatic cuts the mogul chose, or null if they haven't chosen */
	static SegmentFilters readFilters(String json) {
		var edits = parse(json);
		if (edits == null || !(edits.get(FILTERS) instanceof Map<?, ?> f))
			return null;
		return new SegmentFilters(Boolean.TRUE.equals(f.get("fillers")), Boolean.TRUE.equals(f.get("stutters")),
				Boolean.TRUE.equals(f.get("silences")),
				f.get("silenceLongerThan") instanceof Number n ? n.longValue()
						: SegmentFilters.DEFAULT_SILENCE_LONGER_THAN,
				f.get("silenceLeave") instanceof Number n ? n.longValue() : SegmentFilters.DEFAULT_SILENCE_LEAVE);
	}

	/** the ids of detections the mogul put back */
	static Set<String> readRestored(String json) {
		var edits = parse(json);
		var restored = new HashSet<String>();
		if (edits != null && edits.get(RESTORED) instanceof List<?> ids)
			for (var id : ids)
				restored.add(String.valueOf(id));
		return restored;
	}

	static String write(List<KeptRange> kept) {
		return write(kept, null, Set.of());
	}

	/** null when there's nothing to say, which is how an unedited segment reads */
	static String write(List<KeptRange> kept, SegmentFilters filters, Set<String> restored) {
		var edits = new LinkedHashMap<String, Object>();
		if (kept != null)
			edits.put(KEPT, kept.stream().map(range -> List.of(range.start(), range.end())).toList());
		if (filters != null) {
			var f = new LinkedHashMap<String, Object>();
			f.put("fillers", filters.fillers());
			f.put("stutters", filters.stutters());
			f.put("silences", filters.silences());
			f.put("silenceLongerThan", filters.silenceLongerThan());
			f.put("silenceLeave", filters.silenceLeave());
			edits.put(FILTERS, f);
		}
		if (restored != null && !restored.isEmpty())
			edits.put(RESTORED, restored.stream().sorted().toList());
		return edits.isEmpty() ? null : JsonUtils.write(edits);
	}

	static String forProcessors(List<KeptRange> kept) {
		if (kept == null)
			return "";
		return kept.stream().map(range -> range.start() + "-" + range.end()).collect(Collectors.joining(","));
	}

}
