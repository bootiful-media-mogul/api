package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface PodcastService {

	// todo make this a configuration value from a property file or something.
	String PODCAST_EPISODES_BUCKET = "mogul-managedfiles";

	// String PODCAST_EPISODES_BUCKET = "mogul-podcast-episodes";

	Segment createPodcastEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade);

	void setPodcastEpisodesSegmentTranscript(Long episodeSegmentId, boolean transcribable, String transcript);

	void movePodcastEpisodeSegmentUp(Long episode, Long segment);

	void movePodcastEpisodeSegmentDown(Long episode, Long segment);

	void deletePodcastEpisodeSegment(Long episodeSegmentId);

	Segment getPodcastEpisodeSegmentById(Long episodeSegmentId);

	Map<Long, List<Segment>> getPodcastEpisodeSegmentsByEpisodes(Collection<Long> episodeIds);

	List<Segment> getPodcastEpisodeSegmentsByEpisode(Long id);

	Collection<Podcast> getAllPodcastsByMogul(Long mogulId);

	Collection<Episode> getPodcastEpisodesByPodcast(Long podcastId);

	Podcast createPodcast(Long mogulId, String title);

	Episode createPodcastEpisode(Long podcastId, String title, String description, ManagedFile graphic,
			ManagedFile producedGraphic, ManagedFile producedAudio);

	Podcast getPodcastById(Long podcastId);

	Episode getPodcastEpisodeById(Long episodeId);

	void deletePodcast(Long podcastId);

	void deletePodcastEpisode(Long episodeId);

	Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description);

	Episode updatePodcastEpisodeDraft(Long episodeId, String title, String description);

	void writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId);

	Collection<Episode> getAllPodcastEpisodesByIds(Collection<Long> episodeIds);

}
