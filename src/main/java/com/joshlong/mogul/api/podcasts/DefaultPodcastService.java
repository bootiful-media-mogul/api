package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.ManagedFileUpdatedEvent;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.podcasts.production.MediaNormalizationIntegrationRequest;
import com.joshlong.mogul.api.podcasts.production.MediaNormalizer;
import com.joshlong.mogul.api.utils.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.*;
import java.util.function.BiConsumer;

@Service
@Transactional
class DefaultPodcastService implements PodcastService {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final JdbcClient db;

	private final EpisodeRowMapper episodeRowMapper;

	private final ManagedFileService managedFileService;

	private final MogulService mogulService;

	private final MediaNormalizer mediaNormalizer;

	private final ApplicationEventPublisher publisher;

	DefaultPodcastService(MediaNormalizer mediaNormalizer, MogulService mogulService, JdbcClient db,
			ManagedFileService managedFileService, ApplicationEventPublisher publisher) {
		this.db = db;
		this.mediaNormalizer = mediaNormalizer;
		this.mogulService = mogulService;
		this.managedFileService = managedFileService;
		this.episodeRowMapper = new EpisodeRowMapper(this::getPodcastById, this.managedFileService::getManagedFile);
		this.publisher = publisher;
	}

	@Override
	public List<Segment> getEpisodeSegmentsByEpisode(Long episodeId) {
		var sql = " select * from podcast_episode_segment where podcast_episode_id = ?  order by sequence_number asc  ";
		return this.db.sql(sql)
			.params(episodeId)
			.query(new EpisodeSegmentRowMapper(this.managedFileService::getManagedFile, this::getEpisodeById))
			.list();
	}

	// todo we need something that, whenever a managedfile has been updated, allows us to
	// mark an episode as being completed
	@ApplicationModuleListener
	void podcastManagedFileUpdated(ManagedFileUpdatedEvent managedFileUpdatedEvent) {
		var mf = managedFileUpdatedEvent.managedFile();
		// now find the episode to which this managedFile belongs by looking at the
		// graphic or segments and working backwards

		var sql = """
				select pes.podcast_episode_id as id
				from podcast_episode_segment pes
				where pes.segment_audio_managed_file_id = ?
				UNION
				select pe.id as id
				from podcast_episode pe
				where pe.graphic = ?

				""";

		var all = this.db.sql(sql).params(mf.id(), mf.id()).query((rs, rowNum) -> rs.getLong("id")).set();
		if (!all.isEmpty()) {
			var episodeId = all.iterator().next();
			var episode = getEpisodeById(episodeId);
			var segments = getEpisodeSegmentsByEpisode(episodeId);
			if (episode.graphic().id().equals(mf.id())) { // either its the graphic
				this.mediaNormalizer
					.normalize(new MediaNormalizationIntegrationRequest(episode.graphic(), episode.producedGraphic()));
			} //
			else {
				// or its one of the segments
				segments.stream().filter(s -> s.audio().id().equals(mf.id())).findAny().ifPresent(segment -> {
					var response = this.mediaNormalizer
						.normalize(new MediaNormalizationIntegrationRequest(segment.audio(), segment.producedAudio()));
					Assert.notNull(response, "the response should not be null");
					var updated = new Date();
					// if this is older than the last time we have produced any audio,
					// then we won't reproduce the audio
					db.sql("update podcast_episode  set produced_audio_assets_updated = ? where    id = ? ")
						.params(updated, episodeId)
						.update();
				});
			}
			// once the file has been normalized, we can worry about completeness
			this.refreshPodcastEpisodeCompleteness(episodeId);
		}
	}

	private void refreshPodcastEpisodeCompleteness(Long episodeId) {
		var episode = this.getEpisodeById(episodeId);
		var segments = this.getEpisodeSegmentsByEpisode(episodeId);
		// there must be a graphic managed file, at least one segment, and all segments
		// must have been written
		var written = (episode.graphic().written() && episode.producedGraphic().written()) && !segments.isEmpty()
				&& (segments.stream().allMatch(se -> se.audio().written() && se.producedAudio().written()));

		// log.debug("written? {}", written);
		this.db.sql("update podcast_episode set complete = ? where id = ? ").params(written, episode.id()).update();
		var episodeById = this.getEpisodeById(episode.id());
		for (var e : Set.of(new PodcastEpisodeUpdatedEvent(episodeById),
				new PodcastEpisodeCompletionEvent(episodeById)))
			this.publisher.publishEvent(e);

	}

	@ApplicationModuleListener
	void mogulCreated(MogulCreatedEvent createdEvent) {
		var podcast = this.createPodcast(createdEvent.mogul().id(), createdEvent.mogul().username() + "'s Podcast");
		Assert.notNull(podcast,
				"there should be a newly created podcast associated with the mogul [" + createdEvent.mogul() + "]");
	}

	@Override
	public Collection<Podcast> getAllPodcastsByMogul(Long mogulId) {
		return this.db//
			.sql("select * from podcast where mogul_id = ? order by created")
			.param(mogulId)
			.query(new PodcastRowMapper())
			.list();
	}

	@Override
	public Collection<Episode> getEpisodesByPodcast(Long podcastId) {
		var podcast = this.getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast with id [" + podcastId + "] is null");
		return this.db.sql("select * from podcast_episode where podcast_id =? order by created ")
			.param(podcastId)
			.query(this.episodeRowMapper)
			.list();
	}

	@Override
	public Podcast createPodcast(Long mogulId, String title) {
		var kh = new GeneratedKeyHolder();
		this.db.sql(
				" insert into podcast (mogul_id, title) values (?,?) on conflict on constraint podcast_mogul_id_title_key do update set title = excluded.title ")
			.params(mogulId, title)
			.update(kh);
		var id = JdbcUtils.getIdFromKeyHolder(kh);
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
						podcast_id,
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
		var ep = this.getEpisodeById(id.longValue());
		this.publisher.publishEvent(new PodcastEpisodeCreatedEvent(ep));
		return ep;
	}

	@ApplicationModuleListener
	void podcastDeletedEventNotifyingListener(PodcastDeletedEvent event) {
		var notificationEvent = NotificationEvent.notificationEventFor(event.podcast().mogulId(), event,
				Long.toString(event.podcast().id()), event.podcast().title(), false, true);
		this.publisher.publishEvent(notificationEvent);
	}

	@ApplicationModuleListener
	void podcastCreatedEventNotifyingListener(PodcastCreatedEvent event) {
		var notificationEvent = NotificationEvent.notificationEventFor(event.podcast().mogulId(), event,
				Long.toString(event.podcast().id()), event.podcast().title(), false, true);
		this.publisher.publishEvent(notificationEvent);
	}

	@Override
	public Episode getEpisodeById(Long episodeId) {
		var res = db.sql("select * from podcast_episode where id =?").param(episodeId).query(episodeRowMapper).list();
		return res.isEmpty() ? null : res.getFirst();
	}

	private void updateEpisodeSegmentOrder(Long episodeSegmentId, int order) {
		log.debug("updating podcast_episode_segment [{}] to sequence_number : {}", episodeSegmentId, order);
		this.db.sql("update podcast_episode_segment set sequence_number = ? where id = ?")
			.params(order, episodeSegmentId)
			.update();

	}

	/**
	 * @param position the delta in position: -1 if the item is to be moved earlier in the
	 * collection, +1 if it's to be moved later.
	 */
	private void moveEpisodeSegment(Long episodeId, Long segmentId, int position) {
		var segments = getEpisodeSegmentsByEpisode(episodeId);
		var segment = getEpisodeSegmentById(segmentId);
		var positionOfSegment = segments.indexOf(segment);
		var newPositionOfSegment = positionOfSegment + position;

		// log.debug("current:{}", positionOfSegment);
		// log.debug("new:{}", newPositionOfSegment);

		if (newPositionOfSegment < 0 || newPositionOfSegment > (segments.size() - 1)) {
			log.debug("you're trying to move out of bounds");
			return;
		}
		segments.remove(segment);
		segments.add(newPositionOfSegment, segment);

		this.reorderSegments(segments);
	}

	private void reorderSegments(List<Segment> segments) {
		var ctr = 0;
		for (var s : segments) {
			ctr += 1;
			updateEpisodeSegmentOrder(s.id(), ctr);
		}
	}

	@Override
	public void movePodcastEpisodeSegmentDown(Long episode, Long segment) {
		moveEpisodeSegment(episode, segment, 1);
	}

	@Override
	public void movePodcastEpisodeSegmentUp(Long episode, Long segment) {
		this.moveEpisodeSegment(episode, segment, -1);
	}

	@Override
	public void deletePodcastEpisodeSegment(Long episodeSegmentId) {
		var segment = this.getEpisodeSegmentById(episodeSegmentId);
		Assert.state(segment != null, "you must specify a valid " + Segment.class.getName());
		var managedFilesToDelete = Set.of(segment.audio().id(), segment.producedAudio().id());
		this.db.sql("delete from podcast_episode_segment where id =?").params(episodeSegmentId).update();
		for (var managedFileId : managedFilesToDelete)
			this.managedFileService.deleteManagedFile(managedFileId);
		this.reorderSegments(this.getEpisodeSegmentsByEpisode(segment.episode().id()));
		this.refreshPodcastEpisodeCompleteness(segment.episode().id());
	}

	@Override
	public void deletePodcast(Long podcastId) {
		var podcast = this.getPodcastById(podcastId);
		for (var episode : this.getEpisodesByPodcast(podcastId)) {
			deletePodcastEpisode(episode.id());
		}
		this.db.sql("delete from podcast where id= ?").param(podcastId).update();
		this.publisher.publishEvent(new PodcastDeletedEvent(podcast));
	}

	@Override
	public void deletePodcastEpisode(Long episodeId) {
		var segmentsForEpisode = this.getEpisodeSegmentsByEpisode(episodeId);
		if (segmentsForEpisode == null)
			segmentsForEpisode = new ArrayList<>();

		var func = (BiConsumer<ManagedFile, Set<Long>>) (mf, ids) -> {
			if (mf != null)
				ids.add(mf.id());
		};

		var episode = this.getEpisodeById(episodeId);

		// todo delete podcast episode
		var ids = new HashSet<Long>();
		func.accept(episode.graphic(), ids);
		func.accept(episode.producedAudio(), ids);
		func.accept(episode.producedGraphic(), ids);

		for (var segment : segmentsForEpisode) {
			func.accept(segment.audio(), ids);
			func.accept(segment.producedAudio(), ids);
		}

		this.db.sql("delete from podcast_episode_segment where podcast_episode_id= ?").param(episode.id()).update();
		this.db.sql("delete from podcast_episode where id= ?").param(episode.id()).update();

		for (var mfId : ids)
			this.managedFileService.deleteManagedFile(mfId);

		this.publisher.publishEvent(new PodcastEpisodeDeletedEvent(episode));
	}

	@Override
	public Podcast getPodcastById(Long podcastId) {
		return db.sql("select * from podcast where id = ?").param(podcastId).query(new PodcastRowMapper()).single();
	}

	@Override
	public Segment createEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade) {
		var maxOrder = (db
			.sql("select  max( sequence_number) from podcast_episode_segment where podcast_episode_id = ? ")
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
						podcast_episode_id,
						segment_audio_managed_file_id,
						produced_segment_audio_managed_file_id,
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
					) ;
				""";
		var segmentAudioManagedFile = managedFileService.createManagedFile(mogulId, bucket, uid, "", 0,
				CommonMediaTypes.MP3);
		var producedSegmentAudioManagedFile = managedFileService.createManagedFile(mogulId, bucket, uid, "", 0,
				CommonMediaTypes.MP3);

		var gkh = new GeneratedKeyHolder();
		this.db.sql(sql)
			.params(episodeId, segmentAudioManagedFile.id(), producedSegmentAudioManagedFile.id(), crossfade, name,
					maxOrder)
			.update(gkh);
		var id = JdbcUtils.getIdFromKeyHolder(gkh);
		reorderSegments(getEpisodeSegmentsByEpisode(episodeId));
		refreshPodcastEpisodeCompleteness(episodeId);
		return this.getEpisodeSegmentById(id.longValue());
	}

	@Override
	public Segment getEpisodeSegmentById(Long episodeSegmentId) {
		var rowMapper = new EpisodeSegmentRowMapper(this.managedFileService::getManagedFile, this::getEpisodeById);
		return this.db//
			.sql("select * from podcast_episode_segment where id =?")//
			.params(episodeSegmentId)
			.query(rowMapper)//
			.optional()//
			.orElse(null);
	}

	@Override
	public Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description) {
		var uid = UUID.randomUUID().toString();
		var podcast = getPodcastById(podcastId);
		Assert.notNull(podcast, "the podcast is null!");
		var bucket = PodcastService.PODCAST_EPISODES_BUCKET;
		var image = managedFileService.createManagedFile(currentMogulId, bucket, uid, "", 0, CommonMediaTypes.BINARY);
		var producedGraphic = managedFileService.createManagedFile(currentMogulId, bucket, uid, "produced-graphic.jpg",
				0, CommonMediaTypes.JPG);
		var producedAudio = managedFileService.createManagedFile(currentMogulId, bucket, uid, "produced-audio.mp3", 0,
				CommonMediaTypes.MP3);
		var episode = createPodcastEpisode(podcastId, title, description, image, producedGraphic, producedAudio);

		this.createEpisodeSegment(currentMogulId, episode.id(), "", 0); // create the one
																		// mandatory
																		// required
																		// segment

		return getEpisodeById(episode.id());
	}

	@Override
	public Episode updatePodcastEpisodeDraft(Long episodeId, String title, String description) {
		Assert.notNull(episodeId, "the episode is null");
		Assert.hasText(title, "the title is null");
		Assert.hasText(description, "the description is null");
		var episode = getEpisodeById(episodeId);
		mogulService.assertAuthorizedMogul(episode.podcast().mogulId());
		db.sql("update podcast_episode set title =? , description =? where  id = ?")
			.params(title, description, episodeId)
			.update();
		var episodeById = getEpisodeById(episodeId);
		Assert.notNull(episodeById, "the result should not be null");
		publisher.publishEvent(new PodcastEpisodeUpdatedEvent(episodeById));
		return episodeById;
	}

	@Override
	public Episode writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId) {
		try {
			this.managedFileService.refreshManagedFile(managedFileId);
			this.db.sql("update podcast_episode set produced_audio_updated=? where id=? ")
				.params(new Date(), episodeId)
				.update();
			log.debug("updated episode {} to have non-null produced_audio_updated", episodeId);
			return this.getEpisodeById(episodeId);
		} //
		catch (Throwable throwable) {
			throw new RuntimeException("got an exception dealing with " + throwable.getLocalizedMessage(), throwable);
		}
	}

}
