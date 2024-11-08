package com.joshlong.mogul.api.blogs;

import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public record Post(Long mogulId, Long id, String title, Date created, String content, String[] tags, boolean complete,
		Map<String, ManagedFile> assets) implements Publishable {
	@Override
	public Serializable publicationKey() {
		return this.id;
	}
}
