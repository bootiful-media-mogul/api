package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.Analyzable;
import com.joshlong.mogul.api.Notable;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.compositions.Composable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Date;
import java.util.Map;

public record Post(Long mogulId, Long id, String title, Date created, String content, boolean complete, boolean visible,
		Map<String, ManagedFile> assets, String summary, String rssSlug,
		Long blogId) implements Notable, Searchable, Publishable, Composable, Analyzable {

	@Override
	public Long publishableId() {
		return this.id;
	}

	@Override
	public Long searchableId() {
		return this.id;
	}

	@Override
	public Long notableKey() {
		return this.id;
	}

	@Override
	public Long compositionKey() {
		return this.id;
	}

	@Override
	public Long analyzableKey() {
		return this.id;
	}

}
