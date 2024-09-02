package com.joshlong.mogul.api.podcasts.transcription;

import com.joshlong.mogul.api.Transcribable;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.Segment;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * submits a podcast episode segment for transcription
 */
// todo hook this into the completness lifecycle event in the podcasts package

class PodcastEpisodeSegmentTranscribable implements Transcribable {

	private final Segment segment;

	private final ManagedFileService managedFileService;

	PodcastEpisodeSegmentTranscribable(Segment segment, ManagedFileService managedFileService) {
		this.segment = segment;
		this.managedFileService = managedFileService;
	}

	@Override
	public Serializable key() {
		return "podcast-episode-segment-" + this.segment.id();
	}

	@Override
	public Resource audio() {
		var managedFile = this.segment.producedAudio();
		Assert.state(managedFile.written(),
				"the managed file with id #" + managedFile.id() + "hasn't been written, can't transcribe!");
		return this.managedFileService.read(managedFile.id());
	}

}