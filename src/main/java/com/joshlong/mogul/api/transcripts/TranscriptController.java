package com.joshlong.mogul.api.transcripts;

import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import com.joshlong.mogul.api.Transcript;
import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

@Controller
class TranscriptController {

	private final TranscriptService transcriptService;

	private final MogulService mogulService;

	TranscriptController(TranscriptService transcriptService, MogulService mogulService) {
		this.transcriptService = transcriptService;
		this.mogulService = mogulService;
	}

	// ids are sequential, so they're easy to guess: check it's the signed-in mogul's
	private Transcript ownedTranscript(Long transcriptId) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		var transcript = transcriptId == null ? null : this.transcriptService.transcriptById(transcriptId);
		Assert.state(transcript != null && mogulId.equals(transcript.mogulId()),
				() -> "there is no transcript #" + transcriptId + " for mogul #" + mogulId);
		return transcript;
	}

	@MutationMapping
	boolean refreshTranscript(@Argument Long transcriptId) {
		var transcript = this.ownedTranscript(transcriptId);
		this.transcriptService.transcribe(transcript.mogulId(), transcript.id());
		return true;
	}

	@MutationMapping
	boolean writeTranscript(@Argument Long transcriptId, @Argument String transcript) {
		this.ownedTranscript(transcriptId);
		this.transcriptService.writeTranscript(transcriptId, transcript);
		return true;
	}

}
