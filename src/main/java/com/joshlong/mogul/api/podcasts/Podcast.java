package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.compositions.Composable;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public record Podcast(Long mogulId, Long id, String title, Date created, List<Episode> episodes) implements Composable {

	@Override
	public Serializable compositionKey() {
		return this.id();
	}
}
