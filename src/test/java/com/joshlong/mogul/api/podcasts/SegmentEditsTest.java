package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.utils.JsonUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class SegmentEditsTest {

	// JsonUtils keeps the mapper Spring hands it in a static; without a context, hand it
	// one directly.
	@BeforeAll
	static void jsonUtils() throws Exception {
		var constructor = JsonUtils.class.getDeclaredConstructor(JsonMapper.class);
		constructor.setAccessible(true);
		constructor.newInstance(JsonMapper.builder().build());
	}

	private final List<KeptRange> kept = List.of(new KeptRange(0, 30_000), new KeptRange(60_000, 234_960));

	@Test
	void roundTripsThroughTheColumn() {
		var json = SegmentEdits.write(this.kept);
		assertThat(json).isEqualTo("{\"kept\":[[0,30000],[60000,234960]]}");
		assertThat(SegmentEdits.read(json)).isEqualTo(this.kept);
	}

	@Test
	void anUneditedSegmentIsNullBothWays() {
		assertThat(SegmentEdits.write(null)).isNull();
		assertThat(SegmentEdits.read(null)).isNull();
		assertThat(SegmentEdits.read("")).isNull();
	}

	@Test
	void writesWhatTheProcessorsRead() {
		assertThat(SegmentEdits.forProcessors(this.kept)).isEqualTo("0-30000,60000-234960");
		assertThat(SegmentEdits.forProcessors(null)).isEmpty();
	}

	@Test
	void aRangeHasToGoForwards() {
		assertThatIllegalStateException().isThrownBy(() -> new KeptRange(500, 100));
		assertThatIllegalStateException().isThrownBy(() -> new KeptRange(-1, 100));
	}

}
