package com.joshlong.mogul.api.managedfiles;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteRangeTest {

	private static final long SIZE = 57_630_670;

	@Test
	void noHeaderIsTheWholeFile() {
		assertThat(ByteRange.of(null, SIZE)).isNull();
		assertThat(ByteRange.of(" ", SIZE)).isNull();
	}

	@Test
	void anOpenEndedRangeRunsToTheEnd() {
		var range = ByteRange.of("bytes=0-", SIZE);
		assertThat(range.start()).isZero();
		assertThat(range.end()).isEqualTo(SIZE - 1);
		assertThat(range.length()).isEqualTo(SIZE);
		assertThat(range.contentRange()).isEqualTo("bytes 0-57630669/57630670");
	}

	@Test
	void aClosedRange() {
		var range = ByteRange.of("bytes=1000-1999", SIZE);
		assertThat(range.length()).isEqualTo(1000);
		assertThat(range.contentRange()).isEqualTo("bytes 1000-1999/57630670");
	}

	@Test
	void anEndPastTheFileStopsAtTheFile() {
		var range = ByteRange.of("bytes=100-999999999", SIZE);
		assertThat(range.end()).isEqualTo(SIZE - 1);
	}

	@Test
	void aSuffixRangeIsTheLastBytes() {
		var range = ByteRange.of("bytes=-500", SIZE);
		assertThat(range.start()).isEqualTo(SIZE - 500);
		assertThat(range.end()).isEqualTo(SIZE - 1);
	}

	@Test
	void aStartPastTheFileIsUnsatisfiable() {
		assertThat(ByteRange.of("bytes=" + SIZE + "-", SIZE)).isSameAs(ByteRange.UNSATISFIABLE);
		assertThat(ByteRange.of("bytes=0-", 0)).isSameAs(ByteRange.UNSATISFIABLE);
	}

	@Test
	void malformedOrSeveralRangesAreTheWholeFile() {
		assertThat(ByteRange.of("bytes=abc", SIZE)).isNull();
		assertThat(ByteRange.of("items=0-10", SIZE)).isNull();
		assertThat(ByteRange.of("bytes=0-10,20-30", SIZE)).isNull();
	}

}
