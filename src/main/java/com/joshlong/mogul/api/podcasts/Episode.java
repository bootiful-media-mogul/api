package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Analyzable;
import com.joshlong.mogul.api.Notable;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.compositions.Composable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Date;

public record Episode(Long id, Long podcastId, String title, String description, Date created, ManagedFile graphic,
		ManagedFile producedGraphic, ManagedFile producedAudio, boolean complete, Date producedAudioUpdated,
		Date producedAudioAssetsUpdated) implements Notable, Publishable, Composable, Analyzable {

	@Override
	public Long publishableId() {
		return this.id();
	}

	@Override
	public Long compositionKey() {
		return this.id();
	}

	@Override
	public Long notableKey() {
		return this.id();
	}

	@Override
	public Long analyzableKey() {
		return this.id();
	}

}
