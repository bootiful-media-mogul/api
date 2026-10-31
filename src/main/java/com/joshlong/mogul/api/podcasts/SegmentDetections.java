package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.utils.JsonUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * what was found in a segment's cleaned audio, in milliseconds on its timeline -- which
 * is the upload's timeline too, since cleanvoice muted rather than cut.
 *
 * @param cleanvoice what cleanvoice muted, with its kind: {@code FILLER_SOUND},
 * {@code STUTTERING}, {@code DEADAIR}...
 * @param silences the silences of half a second or more
 */
public record SegmentDetections(List<Detection> cleanvoice, List<Detection> silences) {

	public static final String FILLER = "FILLER_SOUND";

	public static final String STUTTER = "STUTTERING";

	public record Detection(long start, long end, String kind) {

		/**
		 * stable for as long as the detections are: a kind and a start can't repeat.
		 */
		public String id() {
			return (this.kind == null ? "silence" : this.kind) + "@" + this.start;
		}

	}

	/**
	 * reads the {@code detections} column, which is exactly what the processors wrote.
	 */
	static SegmentDetections read(String json) {
		if (!StringUtils.hasText(json))
			return null;
		var map = JsonUtils.read(json, new ParameterizedTypeReference<Map<String, List<Map<String, Object>>>>() {
		});
		return new SegmentDetections(detections(map.get("cleanvoice")), detections(map.get("silences")));
	}

	private static List<Detection> detections(List<Map<String, Object>> rows) {
		var detections = new ArrayList<Detection>();
		if (rows != null)
			for (var row : rows)
				detections.add(new Detection(((Number) row.get("start")).longValue(),
						((Number) row.get("end")).longValue(), (String) row.get("kind")));
		return detections;
	}

}
