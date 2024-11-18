package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.compositions.Composable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.io.Serializable;
import java.util.Date;

public record Episode(Long id, Long podcastId, String title, String description, Date created, ManagedFile graphic,
		ManagedFile producedGraphic, ManagedFile producedAudio, boolean complete, Date producedAudioUpdated,
		Date producedAudioAssetsUpdated) implements Publishable, Composable {

	@Override
	public Serializable publicationKey() {
		return this.id();
	}

	@Override
	public Serializable compositionKey() {
		return this.id();
	}
}
