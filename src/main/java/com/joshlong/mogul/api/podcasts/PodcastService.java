package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface PodcastService {

	Composition getPodcastEpisodeTitleComposition(Long episodeId);

	Composition getPodcastEpisodeDescriptionComposition(Long episodeId);

	Segment createPodcastEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade);

	void movePodcastEpisodeSegmentUp(Long episode, Long segment);

	void movePodcastEpisodeSegmentDown(Long episode, Long segment);

	void deletePodcastEpisodeSegment(Long episodeSegmentId);

	Collection<Segment> getPodcastEpisodeSegmentsByIds(List<Long> episodeSegmentIds);

	/**
	 * the segment, provided it belongs to the mogul.
	 */
	Segment getPodcastEpisodeSegmentById(Long mogulId, Long segmentId);

	/**
	 * records which ranges of the segment's uploaded audio to keep, and renders its
	 * produced audio from them. null keeps all of it. the render finishes later, with a
	 * {@code MediaNormalizedEvent}.
	 */
	void editPodcastEpisodeSegment(Long mogulId, Long segmentId, List<KeptRange> kept);

	/**
	 * draws the waveform of the segment's uploaded audio. uploads draw one on their own;
	 * this is for segments from before they did.
	 */
	void requestPodcastEpisodeSegmentWaveform(Long mogulId, Long segmentId);

	Map<Long, List<Segment>> getPodcastEpisodeSegmentsByEpisodes(Collection<Long> episodeIds);

	Map<Long, Long> getPodcastEpisodeDurationsByEpisodes(Collection<Long> episodeIds);

	List<Segment> getPodcastEpisodeSegmentsByEpisode(Long id);

	Collection<Podcast> getAllPodcastsByMogul(Long mogulId);

	Collection<Podcast> getAllPodcastsById(List<Long> mogulIds);

	Collection<Episode> getPodcastEpisodesByPodcast(Long podcastId, boolean deep);

	Podcast createPodcast(Long mogulId, String title);

	Episode createPodcastEpisode(Long podcastId, String title, String description, ManagedFile graphic,
			ManagedFile producedGraphic, ManagedFile producedAudio);

	Podcast getPodcastById(Long podcastId);

	Episode getPodcastEpisodeById(Long episodeId);

	Podcast updatePodcast(Long podcastId, String title);

	void deletePodcast(Long podcastId);

	void deletePodcastEpisode(Long episodeId);

	Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description);

	/**
	 * @param created when the episode should claim to have been created; ignored when
	 * {@literal null}, so callers that only mean to edit the text can pass nothing.
	 */
	Episode updatePodcastEpisodeDetails(Long episodeId, String title, String description, Date created);

	void writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId);

	Collection<Episode> getAllPodcastEpisodesByIds(Collection<Long> episodeIds);

}
