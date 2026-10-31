package com.joshlong.mogul.api.podcasts;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * the cases in {@code segment-cuts/cases.json} are shared with the client, whose editor
 * previews the same cuts. a copy lives in its {@code test/unit}; keep the two identical.
 */
class SegmentCutsTest {

	@TestFactory
	Stream<DynamicTest> everySharedCase() throws Exception {
		var cases = JsonMapper.builder()
			.build()
			.readTree(new ClassPathResource("/segment-cuts/cases.json").getInputStream());
		var tests = new ArrayList<DynamicTest>();
		for (var c : cases)
			tests.add(DynamicTest.dynamicTest(c.get("name").asString(), () -> {
				var result = SegmentCuts.compute(c.get("duration").asLong(), ranges(c.get("kept")),
						detections(c.get("detections")), filters(c.get("filters")), restored(c.get("restored")));
				assertThat(result).containsExactlyElementsOf(ranges(c.get("expected")));
			}));
		return tests.stream();
	}

	private static List<KeptRange> ranges(JsonNode node) {
		if (node == null || node.isNull())
			return null;
		var ranges = new ArrayList<KeptRange>();
		for (var r : node)
			ranges.add(new KeptRange(r.get(0).asLong(), r.get(1).asLong()));
		return ranges;
	}

	private static SegmentDetections detections(JsonNode node) {
		if (node == null || node.isNull())
			return null;
		var cleanvoice = new ArrayList<SegmentDetections.Detection>();
		for (var d : node.get("cleanvoice"))
			cleanvoice.add(new SegmentDetections.Detection(d.get("start").asLong(), d.get("end").asLong(),
					d.get("kind").asString()));
		var silences = new ArrayList<SegmentDetections.Detection>();
		for (var d : node.get("silences"))
			silences.add(new SegmentDetections.Detection(d.get("start").asLong(), d.get("end").asLong(), null));
		return new SegmentDetections(cleanvoice, silences);
	}

	private static SegmentFilters filters(JsonNode f) {
		return new SegmentFilters(f.get("fillers").asBoolean(), f.get("stutters").asBoolean(),
				f.get("silences").asBoolean(), f.get("silenceLongerThan").asLong(), f.get("silenceLeave").asLong());
	}

	private static Set<String> restored(JsonNode node) {
		var restored = new HashSet<String>();
		for (var r : node)
			restored.add(r.asString());
		return restored;
	}

}
