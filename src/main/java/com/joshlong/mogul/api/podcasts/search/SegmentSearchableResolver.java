package com.joshlong.mogul.api.podcasts.search;

import com.joshlong.mogul.api.AbstractSearchableResolver;
import com.joshlong.mogul.api.SearchableResult;
import com.joshlong.mogul.api.Transcribable;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.Segment;

import java.util.*;
import java.util.function.BiFunction;

class SegmentSearchableResolver extends AbstractSearchableResolver<Segment> {

	private final PodcastService podcastService;

	private final BiFunction<Long, Collection<Transcribable>, Map<Transcribable, String>> transcriptLoader;

	SegmentSearchableResolver(Class<Segment> entityClass, PodcastService podcastService,
			BiFunction<Long, Collection<Transcribable>, Map<Transcribable, String>> transcriptLoader) {
		super(entityClass);
		this.podcastService = podcastService;
		this.transcriptLoader = transcriptLoader;
	}

	@Override
	public Segment find(Long searchableId) {
		var s = this.podcastService.getPodcastEpisodeSegmentsByIds(List.of(searchableId));
		if (s.size() == 1)
			return s.iterator().next();
		return null;
	}

	@Override
	public List<SearchableResult<Segment>> results(List<Long> searchableIds) {
		var results = new ArrayList<SearchableResult<Segment>>();
		if (searchableIds.isEmpty())
			return results;
		var segments = this.podcastService.getPodcastEpisodeSegmentsByIds(searchableIds);
		if (segments.isEmpty())
			return results;
		// the episodes, then the podcasts, each in one batch, to find every segment's
		// owner
		var episodesById = new HashMap<Long, Episode>();
		var episodeIds = new HashSet<Long>();
		for (var segment : segments)
			episodeIds.add(segment.episodeId());
		for (var episode : this.podcastService.getAllPodcastEpisodesByIds(episodeIds))
			episodesById.put(episode.id(), episode);
		var podcastIds = episodesById.values().stream().map(Episode::podcastId).distinct().toList();
		var mogulsByPodcast = new HashMap<Long, Long>();
		for (var podcast : this.podcastService.getAllPodcastsById(podcastIds))
			mogulsByPodcast.put(podcast.id(), podcast.mogulId());
		// a batch can span moguls, and transcripts are read one mogul at a time
		var segmentsByMogul = new HashMap<Long, List<Segment>>();
		for (var segment : segments) {
			var episode = episodesById.get(segment.episodeId());
			var mogulId = episode == null ? null : mogulsByPodcast.get(episode.podcastId());
			if (mogulId != null)
				segmentsByMogul.computeIfAbsent(mogulId, _ -> new ArrayList<>()).add(segment);
		}
		for (var entry : segmentsByMogul.entrySet()) {
			var mogulId = entry.getKey();
			var transcribables = entry.getValue().stream().map(s -> (Transcribable) s).toList();
			var transcripts = new HashMap<Long, String>();
			this.transcriptLoader.apply(mogulId, transcribables)
				.forEach((transcribable, transcript) -> transcripts.put(transcribable.transcribableId(), transcript));
			for (var segment : entry.getValue())
				results.add(this.buildResultFor(segment, episodesById.get(segment.episodeId()),
						transcripts.get(segment.id()), mogulId));
		}
		return results;
	}

	@Override
	public Collection<Long> searchableIds(Long mogulId) {
		return this.podcastService.getPodcastEpisodeSegmentIdsByMogul(mogulId);
	}

	private SearchableResult<Segment> buildResultFor(Segment segment, Episode episode, String transcript,
			Long mogulId) {
		return new SearchableResult<>(segment.searchableId(), segment, episode.title(), transcript, episode.id(),
				episode.created(), this.type, mogulId);
	}

}
