package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.ManagedFileUpdatedEvent;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.podcasts.production.MediaNormalizer;
import com.joshlong.mogul.api.transcription.Transcriber;
import com.joshlong.mogul.api.transcription.TranscriptionProcessedEvent;
import com.joshlong.mogul.api.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This service uses a caching scheme that you need to be aware of when effecting changes.
 * All podcasts from all moguls are stored, live, in this class. when there is an update,
 * the entire graph for the mogul is reloaded and kept in {@link ConcurrentHashMap}. The
 * key is to make sure all updates to the SQL DB result in a refreshing of the mogul
 * object graph kept in memory.
 */
@Service
@Transactional
class DefaultPodcastService implements PodcastService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final PodcastRowMapper podcastRowMapper;

	private final SegmentRowMapper episodeSegmentRowMapper;

	private final MogulService mogulService;

	private final ManagedFileService managedFileService;

	private final MediaNormalizer mediaNormalizer;

	private final JdbcClient db;

	private final Transcriber transcriber;

	private final ApplicationEventPublisher publisher;

	DefaultPodcastService(MediaNormalizer mediaNormalizer, MogulService mogulService, JdbcClient db,
			ManagedFileService managedFileService, ApplicationEventPublisher publisher, Transcriber transcriber) {
		this.db = db;
		this.mediaNormalizer = mediaNormalizer;
		this.mogulService = mogulService;
		this.managedFileService = managedFileService;
		this.publisher = publisher;
		this.transcriber = transcriber;
		this.podcastRowMapper = new PodcastRowMapper();
		this.episodeSegmentRowMapper = new SegmentRowMapper(this.managedFileService::getManagedFile);
	}

	@Override
	public Map<Long, List<Segment>> getPodcastEpisodeSegmentsByEpisodes(Collection<Long> episodes) {

		if (episodes.isEmpty())
			return new HashMap<>();

		var idsAsString = episodes.stream().map(e -> Long.toString(e)).collect(Collectors.joining(", "));
		var segments = db
			.sql("select * from podcast_episode_segment pes where pes.podcast_episode  in (" + idsAsString + ") ")
			.query(this.episodeSegmentRowMapper)
			.list();
		var episodeToSegmentsMap = new HashMap<Long, List<Segment>>();
		for (var s : segments) {
			episodeToSegmentsMap.computeIfAbsent(s.episodeId(), sk -> new ArrayList<>()).add(s);
		}
		return episodeToSegmentsMap;

	}

	@Override
	public List<Segment> getPodcastEpisodeSegmentsByEpisode(Long episodeId) {

		var sql = " select * from podcast_episode_segment where podcast_episode  = ? order by sequence_number ASC ";
		var episodeSegmentsFromDb = this.db //
			.sql(sql) //
			.params(episodeId) //
			.query(this.episodeSegmentRowMapper) //
			.stream()//
			.sorted(Comparator.comparingInt(Segment::order))//
			.toList();
		return new ArrayList<>(episodeSegmentsFromDb);
	}
	// todo should i also be doing transcription in this method?
	// this method knows when a file has been written and is in an ideal position
	// to update the record.

	@ApplicationModuleListener
	void podcastManagedFileUpdated(ManagedFileUpdatedEvent managedFileUpdatedEvent) throws Exception {
		var mf = managedFileUpdatedEvent.managedFile();
		var sql = """
				select pes.podcast_episode  as id
				from podcast_episode_segment pes
				where pes.segment_audio_managed_file  = ?
				UNION
				select pe.id as id
				from podcast_episode pe
				where pe.graphic = ?
				""";

		var all = this.db.sql(sql).params(mf.id(), mf.id()).query((rs, rowNum) -> rs.getLong("id")).set();
		if (all.isEmpty())
			return;

		var episodeId = all.iterator().next();
		var episode = this.getPodcastEpisodeById(episodeId);
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);

		var updatedFlag = new AtomicBoolean(false);
		if (episode.graphic().id().equals(mf.id())) { // either it's the graphic..
			updatedFlag.set(true);
			this.mediaNormalizer.normalize(episode.graphic(), episode.producedGraphic());
		} //
		else {
			// or it's one of the segments..
			for (var segment : segments) {
				if (segment.audio().id().equals(mf.id())) {
					this.mediaNormalizer.normalize(segment.audio(), segment.producedAudio());
					// if this is older than the last time we have produced any audio,
					// then we won't reproduce the audio
					this.db.sql("update podcast_episode  set produced_audio_assets_updated = ? where id = ? ")
						.params(new Date(), episodeId)
						.update();

					if (segment.transcribable() && !StringUtils.hasText(segment.transcript())) {
						this.transcribe(segment.id(), Segment.class,
								this.managedFileService.read(segment.producedAudio().id()));
					}
					updatedFlag.set(true);
				}
			}
		}
		// once the file has been normalized, we can worry about completeness
		this.refreshPodcastEpisodeCompleteness(episodeId);
		if (updatedFlag.get()) {
			this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(this.getPodcastEpisodeById(episodeId)));
		}
	}

	private void transcribe(Serializable key, Class<?> subject, Resource resource) {
		var reply = this.transcriber.transcribe(resource);
		var transcriptionProcessedEvent = new TranscriptionProcessedEvent(key, reply, subject);
		this.publisher.publishEvent(transcriptionProcessedEvent);
	}

	@ApplicationModuleListener
	void onPodcastEpisodeSegmentTranscription(TranscriptionProcessedEvent processedEvent) throws Exception {
		var key = processedEvent.key();
		var txt = processedEvent.transcript();
		var clzz = processedEvent.subject();
		if (StringUtils.hasText(txt) && clzz.getName().equals(Segment.class.getName())
				&& key instanceof Number number) {
			var id = number.longValue();
			this.setPodcastEpisodesSegmentTranscript(id, true, txt);
		}

	}

	private void refreshPodcastEpisodeCompleteness(Long episodeId) {
		var episode = this.getPodcastEpisodeById(episodeId);
		var mogulId = episode.producedAudio().mogulId(); // hacky.
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		var graphicsWritten = episode.graphic().written() && episode.producedGraphic().written();
		var complete = graphicsWritten && !segments.isEmpty()
				&& (segments.stream().allMatch(se -> se.audio().written() && se.producedAudio().written()));
		this.db.sql("update podcast_episode set complete = ? where id = ? ").params(complete, episode.id()).update();
		var episodeById = this.getPodcastEpisodeById(episode.id());
		for (var e : Set.of(new PodcastEpisodeUpdatedEvent(episodeById),
				new PodcastEpisodeCompletionEvent(mogulId, episodeById))) {
			this.publisher.publishEvent(e);
		}
	}

	@ApplicationModuleListener
	void mogulCreated(MogulCreatedEvent createdEvent) {
		var mogul = createdEvent.mogul();
		if (this.getAllPodcastsByMogul(mogul.id()).isEmpty()) {
			var podcast = this.createPodcast(mogul.id(), mogul.givenName() + " " + mogul.familyName() + "'s Podcast");
			Assert.notNull(podcast,
					"there should be a newly created podcast associated with the mogul [" + mogul + "]");
		}
	}

	@Override
	public Collection<Episode> getPodcastEpisodesByPodcast(Long podcastId) {
		var podcast = this.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast with id [" + podcastId + "] is null");
		var episodeRowMapper = new EpisodeRowMapper(this.managedFileService::getManagedFile,
				episodeId -> new ArrayList<>());
		var episodes = this.db//
			.sql(" select * from podcast_episode pe where pe.podcast  = ? ") //
			.param(podcastId)//
			.query(episodeRowMapper)//
			.list();
		var episodeHashMap = new HashMap<Long, Episode>();
		for (var episode : episodes)
			episodeHashMap.put(episode.id(), episode);

		var ids = episodes.stream().map(Episode::id).toList();
		var segmentsByEpisodes = this.getPodcastEpisodeSegmentsByEpisodes(ids);
		for (var e : segmentsByEpisodes.entrySet()) {
			var episodeId = e.getKey();
			var segments = e.getValue();
			episodeHashMap.get(episodeId).segments().addAll(segments);
		}
		return episodes;
	}

	@Override
	public Podcast createPodcast(Long mogulId, String title) {
		var generatedKeyHolder = new GeneratedKeyHolder();
		this.db.sql(
				" insert into podcast (mogul , title) values (?,?) on conflict on constraint podcast_mogul_id_title_key do update set title = excluded.title ")
			.params(mogulId, title)
			.update(generatedKeyHolder);
		var id = JdbcUtils.getIdFromKeyHolder(generatedKeyHolder);
		var podcast = this.getPodcastById(id.longValue());
		this.publisher.publishEvent(new PodcastCreatedEvent(podcast));
		return podcast;
	}

	@Override
	public Episode createPodcastEpisode(Long podcastId, String title, String description, ManagedFile graphic,
			ManagedFile producedGraphic, ManagedFile producedAudio) {
		Assert.notNull(podcastId, "the podcast is null");
		Assert.hasText(title, "the title has no text");
		Assert.hasText(description, "the description has no text");
		Assert.notNull(graphic, "the graphic is null ");
		Assert.notNull(producedAudio, "the produced audio is null ");
		Assert.notNull(producedGraphic, "the produced graphic is null");
		var kh = new GeneratedKeyHolder();
		this.db.sql("""
					insert into podcast_episode(
								podcast ,
						title,
						description,
						graphic ,
						produced_graphic,
						produced_audio
					)
					values (
						?,
						?,
						?,
						?,
						?,
						?
					)
				""")
			.params(podcastId, title, description, graphic.id(), producedGraphic.id(), producedAudio.id())
			.update(kh);
		var id = JdbcUtils.getIdFromKeyHolder(kh);
		var episodeId = id.longValue();
		var episode = this.getPodcastEpisodeById(episodeId);
		this.publisher.publishEvent(new PodcastEpisodeCreatedEvent(episode));
		return episode;
	}

	@Override
	public Episode getPodcastEpisodeById(Long episodeId) {
		var erm = new EpisodeRowMapper(managedFileService::getManagedFile, this::getPodcastEpisodeSegmentsByEpisode);//
		var res = this.db //
			.sql("select * from podcast_episode where id =?")//
			.param(episodeId)//
			.query(erm) //
			.list();
		return res.isEmpty() ? null : res.getFirst();
	}

	private void updateEpisodeSegmentOrder(Long episodeSegmentId, int order) {
		this.db //
			.sql("update podcast_episode_segment set sequence_number = ? where id = ?")
			.params(order, episodeSegmentId)
			.update();
	}

	private void moveEpisodeSegment(Long episodeId, Long segmentId, int position) {
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		var segment = this.getPodcastEpisodeSegmentById(segmentId);
		var positionOfSegment = segments.indexOf(segment);
		var newPositionOfSegment = positionOfSegment + position;
		if (newPositionOfSegment < 0 || newPositionOfSegment > (segments.size() - 1)) {
			this.log.debug("you're trying to move out of bounds");
			return;
		}
		segments.remove(segment);
		segments.add(newPositionOfSegment, segment);
		this.reorderSegments(segments);
		var epId = this.getPodcastEpisodeSegmentById(segmentId).episodeId();
		var ep = this.getPodcastEpisodeById(epId);
		this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(ep));
	}

	private void reorderSegments(List<Segment> segments) {
		var counter = 0;
		for (var segment : segments) {
			counter += 1;
			this.updateEpisodeSegmentOrder(segment.id(), counter);
		}
	}

	@Override
	public void movePodcastEpisodeSegmentDown(Long episode, Long segment) {
		this.moveEpisodeSegment(episode, segment, 1);
	}

	@Override
	public void movePodcastEpisodeSegmentUp(Long episode, Long segment) {
		this.moveEpisodeSegment(episode, segment, -1);
	}

	@Override
	public void deletePodcastEpisodeSegment(Long episodeSegmentId) {
		var segment = this.getPodcastEpisodeSegmentById(episodeSegmentId);
		Assert.state(segment != null, "you must specify a valid " + Segment.class.getName());
		var managedFilesToDelete = Set.of(segment.audio().id(), segment.producedAudio().id());
		this.db.sql("delete from podcast_episode_segment where id =?").params(episodeSegmentId).update();
		for (var managedFileId : managedFilesToDelete)
			this.managedFileService.deleteManagedFile(managedFileId);
		this.reorderSegments(this.getPodcastEpisodeSegmentsByEpisode(segment.episodeId()));
		this.refreshPodcastEpisodeCompleteness(segment.episodeId());
	}

	@Override
	public void deletePodcast(Long podcastId) {
		var podcast = this.getPodcastById(podcastId);
		for (var episode : this.getPodcastEpisodesByPodcast(podcastId)) {
			this.deletePodcastEpisode(episode.id());
		}
		this.db.sql("delete from podcast where id= ?").param(podcastId).update();
		this.publisher.publishEvent(new PodcastDeletedEvent(podcast));
	}

	@Override
	public void deletePodcastEpisode(Long episodeId) {
		var segmentsForEpisode = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		if (segmentsForEpisode == null)
			segmentsForEpisode = new ArrayList<>();

		var episode = this.getPodcastEpisodeById(episodeId);

		var ids = new HashSet<Long>();

		for (var managedFile : new ManagedFile[] { episode.graphic(), episode.producedAudio(),
				episode.producedGraphic() })
			if (managedFile != null)
				ids.add(managedFile.id());

		for (var segment : segmentsForEpisode)
			for (var managedFile : new ManagedFile[] { segment.audio(), segment.producedAudio() })
				if (managedFile != null)
					ids.add(managedFile.id());

		this.db.sql("delete from podcast_episode_segment where podcast_episode  = ?").param(episode.id()).update();
		this.db.sql("delete from podcast_episode where id = ?").param(episode.id()).update();

		for (var managedFileId : ids)
			this.managedFileService.deleteManagedFile(managedFileId);

		this.publisher.publishEvent(new PodcastEpisodeDeletedEvent(episode));
	}

	@Override
	public Podcast getPodcastById(Long podcastId) {
		return this.db.sql("select * from podcast p where p.id=?")//
			.param(podcastId)//
			.query(podcastRowMapper)//
			.single();
	}

	@Override
	public Segment createPodcastEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade) {
		var maxOrder = (this.db
			.sql("select max( sequence_number) from podcast_episode_segment where podcast_episode  = ? ")
			.params(episodeId)
			.query(Number.class)
			.optional()
			.orElse(0)
			.longValue()) + 1;
		var uid = UUID.randomUUID().toString();
		var bucket = PodcastService.PODCAST_EPISODES_BUCKET;
		this.mogulService.assertAuthorizedMogul(mogulId);
		var sql = """
					insert into podcast_episode_segment (
						podcast_episode,
						segment_audio_managed_file ,
						produced_segment_audio_managed_file ,
						cross_fade_duration,
						name,
						sequence_number
					)
					values(
						?,
						?,
						?,
						?,
						?,
					 	?
					);
				""";
		var segmentAudioManagedFile = this.managedFileService.createManagedFile(mogulId, bucket, uid, "", 0,
				CommonMediaTypes.MP3);
		var producedSegmentAudioManagedFile = this.managedFileService.createManagedFile(mogulId, bucket, uid, "", 0,
				CommonMediaTypes.MP3);
		var gkh = new GeneratedKeyHolder();
		this.db //
			.sql(sql)
			.params(episodeId, segmentAudioManagedFile.id(), producedSegmentAudioManagedFile.id(), crossfade, name,
					maxOrder)
			.update(gkh);
		var id = JdbcUtils.getIdFromKeyHolder(gkh);
		var episodeSegmentsByEpisode = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		this.reorderSegments(episodeSegmentsByEpisode);
		this.refreshPodcastEpisodeCompleteness(episodeId);
		return this.getPodcastEpisodeSegmentById(id.longValue());
	}

	@Override
	public void setPodcastEpisodesSegmentTranscript(Long episodeSegmentId, boolean transcribable, String transcript) {
		var segment = this.getPodcastEpisodeSegmentById(episodeSegmentId);
		if (null != segment) {
			this.db.sql("update podcast_episode_segment set transcript = ?, transcribable = ?  where id = ? ")
				.params(transcript, transcribable, segment.id())
				.update();
		} //
		else {
			this.log.debug("could not find the podcast episode segment with id: {} ", episodeSegmentId);
		}
	}

	@Override
	public Segment getPodcastEpisodeSegmentById(Long episodeSegmentId) {
		return this.db//
			.sql("select * from podcast_episode_segment where id =?")//
			.params(episodeSegmentId)
			.query(this.episodeSegmentRowMapper)//
			.optional()//
			.orElse(null);
	}

	@Override
	public Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description) {
		var uid = UUID.randomUUID().toString();
		var podcast = this.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast is null!");
		var bucket = PodcastService.PODCAST_EPISODES_BUCKET;
		var image = this.managedFileService.createManagedFile(currentMogulId, bucket, uid, "", 0,
				CommonMediaTypes.BINARY);
		var producedGraphic = this.managedFileService.createManagedFile(currentMogulId, bucket, uid,
				"produced-graphic.jpg", 0, CommonMediaTypes.JPG);
		var producedAudio = this.managedFileService.createManagedFile(currentMogulId, bucket, uid, "produced-audio.mp3",
				0, CommonMediaTypes.MP3);
		var episode = this.createPodcastEpisode(podcastId, title, description, image, producedGraphic, producedAudio);
		var seg = this.createPodcastEpisodeSegment(currentMogulId, episode.id(), "", 0);
		Assert.notNull(seg, "could not create a podcast episode segment for episode " + episode.id());
		return this.getPodcastEpisodeById(episode.id());
	}

	@Override
	public Episode updatePodcastEpisodeDraft(Long episodeId, String title, String description) {
		Assert.notNull(episodeId, "the episode is null");
		Assert.hasText(title, "the title is null");
		Assert.hasText(description, "the description is null");
		this.db.sql("update podcast_episode set title = ?, description =? where id = ?")
			.params(title, description, episodeId)
			.update();
		this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(this.getPodcastEpisodeById(episodeId)));
		return this.getPodcastEpisodeById(episodeId);
	}

	@Override
	public void writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId) {
		try {
			this.managedFileService.refreshManagedFile(managedFileId);
			this.db //
				.sql("update podcast_episode set produced_audio_updated=? where id=? ") //
				.params(new Date(), episodeId) //
				.update();
			this.log.debug("updated episode {} to have non-null produced_audio_updated", episodeId);
			this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(getPodcastEpisodeById(episodeId)));
			this.getPodcastEpisodeById(episodeId);
		} //
		catch (Throwable throwable) {
			throw new RuntimeException("got an exception dealing with " + throwable.getLocalizedMessage(), throwable);
		}
	}

	@Override
	public Collection<Episode> getAllPodcastEpisodesByIds(List<Long> episodeIds) {
		var episodesToSegments = this.getPodcastEpisodeSegmentsByEpisodes(episodeIds);
		var ids = episodeIds.stream().map(e -> Long.toString(e)).collect(Collectors.joining(","));
		return this.db.sql("select * from podcast_episode pe where pe.id in ( " + ids + " )")
			.query(new EpisodeRowMapper(managedFileService::getManagedFile, episodesToSegments::get))
			.list();
	}

	@EventListener
	void podcastDeletedEventNotifyingListener(PodcastDeletedEvent event) {
		var notificationEvent = NotificationEvent.notificationEventFor(event.podcast().mogulId(), event,
				Long.toString(event.podcast().id()), event.podcast().title(), false, true);
		NotificationEvents.notify(notificationEvent);
	}

	@EventListener
	void podcastCreatedEventNotifyingListener(PodcastCreatedEvent event) {
		var notificationEvent = NotificationEvent.notificationEventFor(event.podcast().mogulId(), event,
				Long.toString(event.podcast().id()), event.podcast().title(), false, true);
		NotificationEvents.notify(notificationEvent);
	}

	@Override
	public Collection<Podcast> getAllPodcastsByMogul(Long mogulId) {
		var pds = this.db //
			.sql("select * from podcast p where p.mogul = ?")//
			.param(mogulId)//
			.query(this.podcastRowMapper)//
			.list();
		return pds;
	}

}
