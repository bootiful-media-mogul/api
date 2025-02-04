package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface PodcastService {

	// String PODCAST_EPISODES_BUCKET = "mogul-managedfiles";

	Composition getPodcastEpisodeTitleComposition(Long episodeId);

	Composition getPodcastEpisodeDescriptionComposition(Long episodeId);

	Segment createPodcastEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade);

	// todo could we genericize this so that there's a chain of responsibility on the
	// server side, event listeners, that handle updating a particular type of entity's
	// transcript?
	void setPodcastEpisodesSegmentTranscript(Long episodeSegmentId, boolean transcribable, String transcript);

	// forces the server to re-initialize the transcript for this podcast episode segment.
	// todo could we genericize this so that there's a chain of responsibility on the
	// server side, event listeners, that handle refreshing a particular type of entity's
	// transcript?
	void refreshPodcastEpisodesSegmentTranscript(Long episodeSegmentId);

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

	Podcast updatePodcast(Long podcastId, String title);

	void deletePodcast(Long podcastId);

	void deletePodcastEpisode(Long episodeId);

	Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description);

	Episode updatePodcastEpisodeDetails(Long episodeId, String title, String description);

	void writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId);

	Collection<Episode> getAllPodcastEpisodesByIds(Collection<Long> episodeIds);
}
