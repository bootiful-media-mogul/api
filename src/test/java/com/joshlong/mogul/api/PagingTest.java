package com.joshlong.mogul.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class PagingTest {

	@Test
	void noPagingHasNoSql() {
		var paging = Paging.none();
		Assertions.assertFalse(paging.paged());
		Assertions.assertEquals("", paging.sql());
		Assertions.assertEquals(List.of(), paging.params());
	}

	@Test
	void aLimitAloneIsALimitClause() {
		var paging = Paging.of(10, null);
		Assertions.assertTrue(paging.paged());
		Assertions.assertEquals(" limit ? ", paging.sql());
		Assertions.assertEquals(List.of(10), paging.params());
	}

	@Test
	void anOffsetAloneIsAnOffsetClause() {
		var paging = Paging.of(null, 20);
		Assertions.assertTrue(paging.paged());
		Assertions.assertEquals(" offset ? ", paging.sql());
		Assertions.assertEquals(List.of(20), paging.params());
	}

	@Test
	void aLimitAndAnOffsetAreBothClausesInOrder() {
		var paging = Paging.of(50, 100);
		Assertions.assertEquals(" limit ? offset ? ", paging.sql());
		Assertions.assertEquals(List.of(50, 100), paging.params());
	}

	@Test
	void aNegativeLimitIsRejected() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> Paging.of(-1, null));
	}

	@Test
	void aNegativeOffsetIsRejected() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> Paging.of(null, -1));
	}

}
