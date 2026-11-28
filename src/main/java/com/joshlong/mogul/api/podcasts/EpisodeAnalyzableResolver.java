package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.AbstractAnalyzableResolver;
import com.joshlong.mogul.api.AnalyzableSection;
import com.joshlong.mogul.api.transcripts.TranscriptService;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * an episode's sections are its segments, in the order they play, each keyed by the
 * segment's id, with its transcript as its text. segments with no transcript yet are left
 * out.
 */
@Component
class EpisodeAnalyzableResolver extends AbstractAnalyzableResolver<Episode> {

	private final PodcastService podcastService;

	private final TranscriptService transcriptService;

	EpisodeAnalyzableResolver(PodcastService podcastService, TranscriptService transcriptService) {
		super(Episode.class);
		this.podcastService = podcastService;
		this.transcriptService = transcriptService;
	}

	@Override
	public Episode find(Long key) {
		return this.podcastService.getPodcastEpisodeById(key);
	}

	@Override
	public Long mogulId(Episode analyzable) {
		var podcast = this.podcastService.getPodcastById(analyzable.podcastId());
		return podcast == null ? null : podcast.mogulId();
	}

	@Override
	public List<AnalyzableSection> sections(Episode analyzable) {
		var segments = this.podcastService.getPodcastEpisodeSegmentsByEpisode(analyzable.id())
			.stream()
			.sorted(Comparator.comparingInt(Segment::order))
			.toList();
		// all the transcripts in one go
		var transcripts = this.transcriptService.readTranscripts(this.mogulId(analyzable), segments);
		var sections = new ArrayList<AnalyzableSection>();
		for (var segment : segments) {
			var transcript = transcripts.get(segment);
			if (StringUtils.hasText(transcript))
				sections.add(new AnalyzableSection(Long.toString(segment.id()), segment.name(), transcript.strip()));
		}
		return sections;
	}

}
