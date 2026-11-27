package com.joshlong.mogul.api;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * an optional page of a query: skip {@code offset} rows, then take {@code limit} of them.
 * a {@literal null} on either side leaves that side out, so {@link #none()} is no paging
 * at all.
 */
public record Paging(@Nullable Integer limit, @Nullable Integer offset) {

	public Paging {
		Assert.isTrue(limit == null || limit >= 0, "the limit must not be negative");
		Assert.isTrue(offset == null || offset >= 0, "the offset must not be negative");
	}

	public static Paging of(@Nullable Integer limit, @Nullable Integer offset) {
		return new Paging(limit, offset);
	}

	public static Paging none() {
		return new Paging(null, null);
	}

	public boolean paged() {
		return this.limit != null || this.offset != null;
	}

	/** the sql to append after an {@code order by}, matching {@link #params()} */
	public String sql() {
		var clauses = new ArrayList<String>();
		if (this.limit != null)
			clauses.add("limit ?");
		if (this.offset != null)
			clauses.add("offset ?");
		return clauses.isEmpty() ? "" : " " + String.join(" ", clauses) + " ";
	}

	/** the values for the placeholders in {@link #sql()}, in the same order */
	public List<Object> params() {
		var params = new ArrayList<Object>();
		if (this.limit != null)
			params.add(this.limit);
		if (this.offset != null)
			params.add(this.offset);
		return params;
	}

}
