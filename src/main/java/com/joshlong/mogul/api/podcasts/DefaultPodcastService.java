package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.compositions.Composition;
import com.joshlong.mogul.api.compositions.CompositionService;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.ManagedFileUpdatedEvent;
import com.joshlong.mogul.api.media.MediaNormalizedEvent;
import com.joshlong.mogul.api.media.MediaService;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import com.joshlong.mogul.api.transcripts.TranscriptInvalidatedEvent;
import com.joshlong.mogul.api.transcripts.TranscriptRecordedEvent;
import com.joshlong.mogul.utils.CacheUtils;
import com.joshlong.mogul.utils.CollectionUtils;
import com.joshlong.mogul.utils.JdbcUtils;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

@Transactional
class DefaultPodcastService implements PodcastService {

	static final String PODCAST_EPISODE_CONTEXT_KEY = "podcastEpisodeId";

	static final String PODCAST_EPISODE_SEGMENT_CONTEXT_KEY = "podcastEpisodeSegmentId";

	static final String PODCAST_EPISODE_GRAPHIC_CONTEXT_KEY = "podcastEpisodeGraphicId";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final PodcastRowMapper podcastRowMapper;

	private final CompositionService compositionService;

	private final ManagedFileService managedFileService;

	private final MediaService mediaService;

	private final JdbcClient db;

	private final ApplicationEventPublisher publisher;

	private final Cache podcastCache, podcastEpisodesCache;

	private final TransactionTemplate transactions;

	private final Comparator<Episode> episodeComparator = Comparator.comparing(Episode::created).reversed();

	DefaultPodcastService(CompositionService compositionService, MediaService mediaService, JdbcClient db,
			ManagedFileService managedFileService, ApplicationEventPublisher publisher, Cache podcastCache,
			Cache podcastEpisodesCache, TransactionTemplate transactions) {
		this.podcastEpisodesCache = podcastEpisodesCache;
		this.podcastCache = podcastCache;
		this.compositionService = compositionService;
		this.db = db;
		this.mediaService = mediaService;
		this.managedFileService = managedFileService;
		this.publisher = publisher;
		this.transactions = transactions;
		this.podcastRowMapper = new PodcastRowMapper();
	}

	@Override
	public Map<Long, List<Segment>> getPodcastEpisodeSegmentsByEpisodes(Collection<Long> episodes) {
		if (episodes.isEmpty())
			return new HashMap<>();
		var segmentResultSetExtractor = new SegmentResultSetExtractor( //
				this.managedFileService::getManagedFiles);
		var segments = this.db //
			.sql(" select * from podcast_episode_segment pes where pes.podcast_episode_id = any(?)  ") //
			.params(new SqlArrayValue("bigint", (Object[]) episodes.toArray(Long[]::new)))//
			.query(segmentResultSetExtractor);
		var episodeToSegmentsMap = new HashMap<Long, List<Segment>>();
		for (var s : segments)
			episodeToSegmentsMap.computeIfAbsent(s.episodeId(), _ -> new ArrayList<>()).add(s);
		for (var entry : episodeToSegmentsMap.entrySet())
			orderedSegments(entry.getValue());
		return episodeToSegmentsMap;
	}

	/**
	 * the id breaks the tie. {@link List#sort} is stable, so ordering on the sequence
	 * number alone left two segments that shared one in whatever order the database
	 * happened to return them - an episode assembled differently from one read to the
	 * next. the schema now refuses that pair outright; this makes the read deterministic
	 * regardless.
	 */
	private List<Segment> orderedSegments(List<Segment> segments) {
		segments.sort(Comparator.comparingInt(Segment::order).thenComparing(Segment::id));
		return segments;
	}

	@Override
	public Map<Long, Long> getPodcastEpisodeDurationsByEpisodes(Collection<Long> episodeIds) {
		if (episodeIds.isEmpty())
			return new HashMap<>();
		var durations = new HashMap<Long, Long>();
		this.db //
			.sql("""
					select podcast_episode_id, sum(duration) as duration
					from podcast_episode_segment
					where podcast_episode_id = any(?)
					group by podcast_episode_id
					""") //
			.params(new SqlArrayValue("bigint", (Object[]) episodeIds.toArray(Long[]::new))) //
			.query((rs, _) -> durations.put(rs.getLong("podcast_episode_id"), rs.getLong("duration"))) //
			.list();
		return durations;
	}

	@Override
	public List<Segment> getPodcastEpisodeSegmentsByEpisode(Long episodeId) {
		return this.orderedSegments(db.sql("select * from podcast_episode_segment where podcast_episode_id = ? ") //
			.param(episodeId)
			.query(new SegmentResultSetExtractor(managedFileService::getManagedFiles)));

	}

	private void triggerTranscription(Long mogulId, Long segmentId, String sourceEtag) {
		this.publisher
			.publishEvent(new TranscriptInvalidatedEvent(mogulId, segmentId, Segment.class, sourceEtag, Map.of()));
	}

	@ApplicationModuleListener
	void invalidateCacheBecauseOfTranscriptUpdates(TranscriptRecordedEvent recordedEvent) {
		this.log.debug("you've got your transcript, invalidate ur cache for podcast episodes!");
	}

	@ApplicationModuleListener
	void mediaNormalized(MediaNormalizedEvent normalizedEvent) {
		if (normalizedEvent.context().containsKey(PODCAST_EPISODE_CONTEXT_KEY)) {
			var episodeId = (Long) normalizedEvent.context().get(PODCAST_EPISODE_CONTEXT_KEY);
			this.invalidatePodcastEpisodeCache(episodeId);
			if (normalizedEvent.context().containsKey(PODCAST_EPISODE_SEGMENT_CONTEXT_KEY)) {
				var segmentId = (Long) normalizedEvent.context().get(PODCAST_EPISODE_SEGMENT_CONTEXT_KEY);
				this.db.sql("update podcast_episode set produced_audio_assets_updated = ? where id = ? ")
					.params(new Date(), episodeId)
					.update();
				this.triggerTranscription(normalizedEvent.in().mogulId(), segmentId, normalizedEvent.out().etag());
				// todo
				this.db.sql("update podcast_episode_segment set duration =  ? where id = ? ")
					.params(normalizedEvent.context().getOrDefault(MediaNormalizedEvent.DURATION_IN_MILLISECONDS, 0L),
							segmentId)
					.update();

			}
			this.refreshPodcastEpisodeCompleteness(episodeId);
			this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(this.getPodcastEpisodeById(episodeId)));
		}

	}

	@ApplicationModuleListener
	void podcastManagedFileUpdated(ManagedFileUpdatedEvent managedFileUpdatedEvent) throws Exception {
		var mf = managedFileUpdatedEvent.managedFile();
		var sql = """
				select pes.podcast_episode_id  as id
				from podcast_episode_segment pes
				where pes.segment_audio_managed_file_id  = ?
				UNION
				select pe.id as id
				from podcast_episode pe
				where pe.graphic_managed_file_id = ?
				""";
		var episodeId = CollectionUtils
			.firstOrNull(this.db.sql(sql).params(mf.id(), mf.id()).query((rs, _) -> rs.getLong("id")).set());
		if (episodeId == null) { // not our problem.
			return;
		}
		this.invalidatePodcastEpisodeCache(episodeId);
		var episode = this.getPodcastEpisodeById(episodeId);
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		if (episode.graphic().id().equals(mf.id())) {
			var podcastEpisodeContext = Map.of(PODCAST_EPISODE_CONTEXT_KEY, (Object) episodeId, //
					PODCAST_EPISODE_GRAPHIC_CONTEXT_KEY, episode.graphic().id() //
			);
			this.mediaService.normalize(episode.graphic(), episode.producedGraphic(), podcastEpisodeContext);
		} //
		else {
			// or it's one of the segments
			for (var segment : segments) {
				if (segment.audio().id().equals(mf.id())) {
					// the edits were ranges of the audio this replaces. kept against a
					// different file they would cut it in arbitrary places.
					if (segment.edits() != null) {
						this.db.sql("update podcast_episode_segment set edits = null where id = ?")
							.params(segment.id())
							.update();
						this.invalidatePodcastEpisodeCache(episodeId);
					}
					var context = this.segmentContext(segment);
					this.mediaService.normalize(segment.audio(), segment.producedAudio(), context);
					this.mediaService.waveform(segment.audio(), this.waveformFor(segment), context);
				}
			}
		}

	}

	private Map<String, Object> segmentContext(Segment segment) {
		return Map.of(PODCAST_EPISODE_CONTEXT_KEY, segment.episodeId(), PODCAST_EPISODE_SEGMENT_CONTEXT_KEY,
				segment.id());
	}

	/**
	 * the segment's waveform file, created on the spot for a segment from before there
	 * were waveforms. two of these racing for the same segment both create a file; the
	 * one whose update lands keeps it, and the other deletes its own.
	 */
	private ManagedFile waveformFor(Segment segment) {
		if (segment.waveform() != null)
			return segment.waveform();
		var audio = segment.audio();
		var created = this.managedFileService.createManagedFile(audio.mogulId(), audio.folder(), "waveform.bin", 0,
				CommonMediaTypes.BINARY, false);
		var claimed = this.db.sql(
				"update podcast_episode_segment set waveform_managed_file_id = ? where id = ? and waveform_managed_file_id is null")
			.params(created.id(), segment.id())
			.update();
		this.invalidatePodcastEpisodeCache(segment.episodeId());
		if (claimed == 1)
			return created;
		this.managedFileService.deleteManagedFile(created.id());
		return Objects.requireNonNull(
				CollectionUtils.firstOrNull(this.getPodcastEpisodeSegmentsByIds(List.of(segment.id()))).waveform());
	}

	/**
	 * the segment, if it belongs to the mogul. every segment operation that comes from a
	 * client goes through here first.
	 */
	private Segment ownedSegment(Long mogulId, Long segmentId) {
		var owned = this.db.sql("""
				select count(*) from podcast_episode_segment pes
				join podcast_episode pe on pe.id = pes.podcast_episode_id
				join podcast p on p.id = pe.podcast_id
				where pes.id = ? and p.mogul_id = ?
				""").params(segmentId, mogulId).query(Long.class).single();
		Assert.state(owned == 1,
				() -> "podcast episode segment #" + segmentId + " does not belong to mogul #" + mogulId);
		return CollectionUtils.firstOrNull(this.getPodcastEpisodeSegmentsByIds(List.of(segmentId)));
	}

	@Override
	public Segment getPodcastEpisodeSegmentById(Long mogulId, Long segmentId) {
		return this.ownedSegment(mogulId, segmentId);
	}

	@Override
	public void editPodcastEpisodeSegment(Long mogulId, Long segmentId, List<KeptRange> kept) {
		var segment = this.ownedSegment(mogulId, segmentId);
		Assert.state(kept == null || !kept.isEmpty(), "an edit has to keep something; delete the segment instead");
		Assert.state(segment.audio().written(), "there's no audio to edit until some has been uploaded");
		this.db.sql("update podcast_episode_segment set edits = ?::jsonb where id = ?")
			.params(SegmentEdits.write(kept), segmentId)
			.update();
		this.invalidatePodcastEpisodeCache(segment.episodeId());
		this.markAssetsDirty(segment.episodeId());
		// the produced audio is rendered from the upload and the edits. the reply
		// lands in mediaNormalized, same as an upload's, which updates the duration
		// and the transcript.
		this.mediaService.normalize(segment.audio(), segment.producedAudio(), SegmentEdits.forProcessors(kept),
				this.segmentContext(segment));
	}

	@Override
	public void requestPodcastEpisodeSegmentWaveform(Long mogulId, Long segmentId) {
		var segment = this.ownedSegment(mogulId, segmentId);
		this.mediaService.waveform(segment.audio(), this.waveformFor(segment), this.segmentContext(segment));
	}

	private void refreshPodcastEpisodeCompleteness(Long episodeId) {
		this.transactions.execute(_ -> {
			this.doBroadcastOfEpisodeCompleteness(episodeId);
			return null;
		});
	}

	private void doBroadcastOfEpisodeCompleteness(Long episodeId) {
		this.invalidatePodcastEpisodeCache(episodeId);
		var episode = this.getPodcastEpisodeById(episodeId);
		var mogulId = episode.producedAudio().mogulId(); // hacky.
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		var graphicsWritten = episode.graphic().written() && episode.producedGraphic().written();
		var allSegmentsHaveWrittenAndProducedAudio = segments.stream()
			.allMatch(se -> se.audio().written() && se.producedAudio().written());
		var complete = StringUtils.hasText(episode.title()) && StringUtils.hasText(episode.description())
				&& graphicsWritten && !segments.isEmpty() && allSegmentsHaveWrittenAndProducedAudio;
		this.db.sql("update podcast_episode set complete = ? where id = ? ").params(complete, episode.id()).update();
		this.invalidatePodcastEpisodeCache(episodeId);
		var episodeById = this.getPodcastEpisodeById(episode.id());
		var detailsOnSegments = new StringBuilder();
		if (!allSegmentsHaveWrittenAndProducedAudio) {
			for (var s : segments) {
				detailsOnSegments //
					.append(s.id()) //
					.append(": written audio? ") //
					.append(s.audio().written()) //
					.append(" produced audio? ")//
					.append(s.producedAudio().written()) //
					.append("\n");
			}
		}

		if (this.log.isDebugEnabled()) {
			var msg = Map.of("graphic written", graphicsWritten, "graphic produced",
					episode.producedGraphic().written(), "segments not empty?", !segments.isEmpty(), "has a title",
					StringUtils.hasText(episode.title()), "all segments have written and produced audio",
					allSegmentsHaveWrittenAndProducedAudio, "details on segments", detailsOnSegments.toString());
			var finalMsg = new StringBuilder();
			for (var k : msg.keySet())
				finalMsg.append(k).append(' ').append(msg.get(k)).append(System.lineSeparator());
			this.log.debug(finalMsg.toString());
		}

		for (var e : Set.of(new PodcastEpisodeUpdatedEvent(episodeById),
				new PodcastEpisodeCompletedEvent(mogulId, episodeById))) {
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

	/**
	 * returns a graph of all the episodes for a given podcast. if you specify
	 * {@code deep}, then it'll return a highly complicated graph of objects which will
	 * take considerably longer to load (but will have everything)
	 * @param podcastId the id for which you want to load episodes.
	 * @param deep whether to return the full graph of objects or just the results
	 * sufficient to display the search results
	 */
	@Override
	public Collection<Episode> getPodcastEpisodesByPodcast(Long podcastId, boolean deep) {
		var results = new ArrayList<>(this.db //
			.sql("  select * from podcast_episode pe where pe.podcast_id  = ? ") //
			.param(podcastId) //
			.query(new EpisodeResultSetExtractor(deep, this.managedFileService::getManagedFiles)));
		results.sort(this.episodeComparator);
		return results;
	}

	@Override
	public Podcast createPodcast(Long mogulId, String title) {
		var generatedKeyHolder = new GeneratedKeyHolder();
		this.db.sql("""
						insert into podcast (mogul_id , title) values (?,?)
				    on conflict on constraint podcast_mogul_id_title_key
					do update set title = excluded.title
				""").params(mogulId, title).update(generatedKeyHolder);
		var id = JdbcUtils.getIdFromKeyHolder(generatedKeyHolder);
		var podcast = this.getPodcastById(id.longValue());
		this.publisher.publishEvent(new PodcastCreatedEvent(podcast));
		return podcast;
	}

	@Override
	public Podcast updatePodcast(Long podcastId, String title) {
		this.db.sql(" update podcast set title = ? where id = ? ").params(title, podcastId).update();
		this.invalidatePodcastCache(podcastId);
		var podcast = this.getPodcastById(podcastId);
		Assert.state((null != podcast.title() && title != null), "you must provide a valid title");
		Assert.state(title.equals(podcast.title()), "you must provide a valid title");
		this.invalidatePodcastCache(podcastId);
		this.publisher.publishEvent(new PodcastUpdatedEvent(podcast));
		return podcast;
	}

	@Override
	public Episode createPodcastEpisode(Long podcastId, String title, String description, ManagedFile graphic,
			ManagedFile producedGraphic, ManagedFile producedAudio) {
		Assert.notNull(podcastId, "the podcast is null");
		Assert.notNull(graphic, "the graphic is null ");
		Assert.notNull(producedAudio, "the produced audio is null ");
		Assert.notNull(producedGraphic, "the produced graphic is null");
		var kh = new GeneratedKeyHolder();
		this.db.sql("""
				insert into podcast_episode(
				podcast_id,
				title,
				description,
				graphic_managed_file_id ,
				produced_graphic_managed_file_id,
				produced_audio_managed_file_id
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
		this.invalidatePodcastEpisodeCache(episodeId);
		this.publisher.publishEvent(new PodcastEpisodeCreatedEvent(episode));
		return episode;
	}

	@Override
	public Episode getPodcastEpisodeById(Long episodeId) {
		var all = this.getAllPodcastEpisodesByIds(List.of(episodeId));
		Assert.notNull(all, "the collection should not be null");
		if (all.isEmpty())
			return null;
		return all.iterator().next();
	}

	private void updateEpisodeSegmentOrder(Long episodeSegmentId, int order) {
		this.db //
			.sql("update podcast_episode_segment set sequence_number = ? where id = ?")
			.params(order, episodeSegmentId)
			.update();
	}

	private void moveEpisodeSegment(Long episodeId, Long segmentId, int position) {
		var segments = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		var segment = CollectionUtils.firstOrNull(this.getPodcastEpisodeSegmentsByIds(List.of(segmentId)));
		var positionOfSegment = segments.indexOf(segment);
		var newPositionOfSegment = positionOfSegment + position;
		if (newPositionOfSegment < 0 || newPositionOfSegment > (segments.size() - 1)) {
			this.log.debug("you're trying to move out of bounds");
			return;
		}
		segments.remove(segment);
		segments.add(newPositionOfSegment, segment);
		this.reorderSegments(segments);
		this.markAssetsDirty(episodeId);
		this.invalidatePodcastEpisodeCache(episodeId);
		var ep = this.getPodcastEpisodeById(episodeId);
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
		var segment = CollectionUtils.firstOrNull(this.getPodcastEpisodeSegmentsByIds(List.of(episodeSegmentId)));
		Assert.state(segment != null, "you must specify a valid " + Segment.class.getName());
		var managedFilesToDelete = new HashSet<>(Set.of(segment.audio().id(), segment.producedAudio().id()));
		if (segment.waveform() != null)
			managedFilesToDelete.add(segment.waveform().id());
		this.markPodcastEpisodeBySegmentAssetsDirty(episodeSegmentId);
		this.db.sql("delete from podcast_episode_segment where id =?").params(episodeSegmentId).update();
		for (var managedFileId : managedFilesToDelete)
			this.managedFileService.deleteManagedFile(managedFileId);
		this.reorderSegments(this.getPodcastEpisodeSegmentsByEpisode(segment.episodeId()));

		this.refreshPodcastEpisodeCompleteness(segment.episodeId());
	}

	@Override
	public void deletePodcast(Long podcastId) {
		var podcast = this.getPodcastById(podcastId);
		for (var episode : this.getPodcastEpisodesByPodcast(podcastId, true)) {
			this.deletePodcastEpisode(episode.id());
		}
		this.db.sql(" delete from podcast where id = ? ").param(podcastId).update();
		this.invalidatePodcastCache(podcastId);
		this.publisher.publishEvent(new PodcastDeletedEvent(podcast));
	}

	private void invalidatePodcastEpisodeCache(Long episodeId) {
		this.podcastEpisodesCache.evictIfPresent(episodeId);
	}

	private void invalidatePodcastCache(Long podcastId) {
		this.podcastCache.evictIfPresent(podcastId);
	}

	@Override
	public void deletePodcastEpisode(Long episodeId) {

		try {
			this.invalidatePodcastEpisodeCache(episodeId);

			this.log.info("deleting episode with id = {}", episodeId);
			var segmentsForEpisode = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
			if (segmentsForEpisode == null)
				segmentsForEpisode = new ArrayList<>();

			var episode = this.getPodcastEpisodeById(episodeId);
			var podcastId = episode.podcastId();
			var ids = new HashSet<Long>();

			for (var managedFile : new ManagedFile[] { episode.graphic(), episode.producedAudio(),
					episode.producedGraphic() })
				if (managedFile != null)
					ids.add(managedFile.id());

			for (var segment : segmentsForEpisode)
				for (var managedFile : new ManagedFile[] { segment.audio(), segment.producedAudio() })
					if (managedFile != null)
						ids.add(managedFile.id());

			var deleted = this.db.sql("delete from podcast_episode_segment where podcast_episode_id  = ?")
				.param(episodeId)
				.update();
			this.log.info("deleted {} segments for episode {}", deleted, episodeId);
			this.db.sql("delete from podcast_episode where id = ?").param(episode.id()).update();

			for (var managedFileId : ids) {
				// this.debug(managedFileId);
				this.managedFileService.deleteManagedFile(managedFileId);
			}
			this.invalidatePodcastCache(podcastId);
			this.invalidatePodcastEpisodeCache(episodeId);
			this.publisher.publishEvent(new PodcastEpisodeDeletedEvent(episode));

			// does the episode still exist?
			var refreshedEpisode = this.getPodcastEpisodeById(episodeId);

			this.log.debug("got the episode after deletion? {}", refreshedEpisode);
		} //
		catch (Throwable throwable) {
			this.log.error("failed to delete episode with id = {}", episodeId, throwable);
		}
	}

	private void debug(Long managedFileId) {
		var refs = this.db.sql("""
				SELECT 'segment' as source, id, podcast_episode_id
				FROM podcast_episode_segment
				WHERE segment_audio_managed_file_id = ? OR produced_segment_audio_managed_file_id = ?
				UNION ALL
				SELECT 'episode' as source, id, id as podcast_episode_id
				FROM podcast_episode
				WHERE graphic_managed_file_id = ?
				   OR produced_audio_managed_file_id = ?
				   OR produced_graphic_managed_file_id = ?
				""")
			.params(managedFileId, managedFileId, managedFileId, managedFileId, managedFileId)
			.query((rs, row) -> rs.getString("source") + " id=" + rs.getLong("id") + " episode="
					+ rs.getLong("podcast_episode_id"))
			.list();

		this.log.info("Before deleting managed_file {}, still referenced by: {}", managedFileId, refs);
	}

	@Override
	public Podcast getPodcastById(Long podcastId) {
		return this.podcastCache.get(podcastId, () -> this.db //
			.sql("select * from podcast p where p.id=?")//
			.param(podcastId)//
			.query(this.podcastRowMapper)//
			.single());
	}

	@Override
	public Composition getPodcastEpisodeTitleComposition(Long episodeId) {
		return this.compositionFor(episodeId, "title");
	}

	@Override
	public Composition getPodcastEpisodeDescriptionComposition(Long episodeId) {
		return this.compositionFor(episodeId, "description");
	}

	private Composition compositionFor(Long episodeId, String field) {
		// a composition is identified by the class and the key alone, so there's nothing
		// to be gained from loading the episode just to read its id back off it.
		return this.compositionService.compose(Episode.class, episodeId, field);
	}

	@Override
	public Segment createPodcastEpisodeSegment(Long mogulId, Long episodeId, String name, long crossfade) {
		// the number this claims is read and then written, so two of these running at
		// once on the same episode would both read the same max and both take the number
		// after it. locking the episode row makes them take turns. it is the episode and
		// not the table, so anyone working on a different episode is unaffected, and it
		// is held only until this transaction commits.
		this.db.sql("select id from podcast_episode where id = ? for update")
			.params(episodeId)
			.query(Long.class)
			.optional();
		var maxOrder = (this.db
			.sql("select max( sequence_number) from podcast_episode_segment where podcast_episode_id  = ? ")
			.params(episodeId)
			.query(Number.class)
			.optional()
			.orElse(0)
			.longValue()) + 1;
		var uid = UUID.randomUUID().toString();
		var sql = """
				insert into podcast_episode_segment (
				podcast_episode_id,
				segment_audio_managed_file_id ,
				produced_segment_audio_managed_file_id  ,
				waveform_managed_file_id,
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
				?,
				?
				);
				""";
		var segmentAudioManagedFile = this.managedFileService.createManagedFile(mogulId, uid, "", 0,
				CommonMediaTypes.MP3, false);
		var producedSegmentAudioManagedFile = this.managedFileService.createManagedFile(mogulId, uid, "", 0,
				CommonMediaTypes.MP3, false);
		var waveformManagedFile = this.managedFileService.createManagedFile(mogulId, uid, "waveform.bin", 0,
				CommonMediaTypes.BINARY, false);
		var gkh = new GeneratedKeyHolder();
		this.db //
			.sql(sql)
			.params(episodeId, segmentAudioManagedFile.id(), producedSegmentAudioManagedFile.id(),
					waveformManagedFile.id(), crossfade, name, maxOrder)
			.update(gkh);
		var id = JdbcUtils.getIdFromKeyHolder(gkh);
		this.invalidatePodcastEpisodeCache(episodeId);
		var episodeSegmentsByEpisode = this.getPodcastEpisodeSegmentsByEpisode(episodeId);
		this.reorderSegments(episodeSegmentsByEpisode);
		this.refreshPodcastEpisodeCompleteness(episodeId);
		this.markAssetsDirty(episodeId);
		this.invalidatePodcastEpisodeCache(episodeId);
		return CollectionUtils.firstOrNull(this.getPodcastEpisodeSegmentsByIds(List.of(id.longValue())));
	}

	private void markPodcastEpisodeBySegmentAssetsDirty(Long podcastEpisodeSegmentId) {
		var pes = this.db //
			.sql("select pes.podcast_episode_id pid from podcast_episode_segment pes  where pes.id = ? ") //
			.params(podcastEpisodeSegmentId)
			.query((rs, _) -> rs.getLong("pid"))
			.single();
		this.markAssetsDirty(pes);
	}

	/**
	 * any deletion, update, or re-ordering should result in a dirty
	 * produced_audio_assets_updated field
	 */
	private void markAssetsDirty(Long episodeId) {
		log.debug("marking the produced_audio_assets_updated = now() for episode_id = {}", episodeId);
		this.db.sql("update podcast_episode set produced_audio_assets_updated  = now() where id   = ?")
			.params(episodeId)
			.update();
	}

	@Override
	public Collection<Segment> getPodcastEpisodeSegmentsByIds(List<Long> episodeSegmentIds) {
		if (episodeSegmentIds.isEmpty())
			return new ArrayList<>();
		var arr = new Long[episodeSegmentIds.size()];
		for (var i = 0; i < episodeSegmentIds.size(); i++) {
			arr[i] = episodeSegmentIds.get(i);
		}
		var segmentList = db.sql("select * from podcast_episode_segment where id = any(?) ") //
			.params(new SqlArrayValue("bigint", (Object[]) arr))//
			.query(new SegmentResultSetExtractor(managedFileService::getManagedFiles));
		// join() runs whether or not debug is on -- slf4j defers formatting, not the
		// evaluation of its arguments -- so guard the one argument that costs something.
		if (this.log.isDebugEnabled())
			this.log.debug("segments returned for episode IDs {}: {}", CollectionUtils.join(episodeSegmentIds, ","),
					segmentList.size());
		return segmentList;
	}

	@Override
	public Episode createPodcastEpisodeDraft(Long currentMogulId, Long podcastId, String title, String description) {
		this.ensurePodcastBelongsToMogul(currentMogulId, podcastId);
		var uid = UUID.randomUUID().toString();
		var image = this.managedFileService.createManagedFile(currentMogulId, uid, "", 0, CommonMediaTypes.BINARY,
				true);
		var producedGraphic = this.managedFileService.createManagedFile(currentMogulId, uid, "produced-graphic.jpg", 0,
				CommonMediaTypes.JPG, true);
		var producedAudio = this.managedFileService.createManagedFile(currentMogulId, uid, "produced-audio.mp3", 0,
				CommonMediaTypes.MP3, true);
		var episode = this.createPodcastEpisode(podcastId, title, description, image, producedGraphic, producedAudio);
		var episodeId = episode.id();
		var titleComp = this.getPodcastEpisodeTitleComposition(episodeId);
		var descriptionComp = this.getPodcastEpisodeDescriptionComposition(episodeId);
		Assert.notNull(titleComp, "the title composition must not be null");
		Assert.notNull(descriptionComp, "the description composition must not be null");
		var seg = this.createPodcastEpisodeSegment(currentMogulId, episodeId, "", 0);
		Assert.notNull(seg, "could not create a podcast episode segment for episode " + episodeId);
		this.invalidatePodcastEpisodeCache(episodeId);
		return this.getPodcastEpisodeById(episodeId);
	}

	private void ensurePodcastBelongsToMogul(Long currentMogulId, Long podcastId) {
		var match = this.db.sql("select p.id as id from podcast p where p.id =  ? and p.mogul_id = ?  ")
			.params(podcastId, currentMogulId)
			.query((rs, rowNum) -> rs.getInt("id"))
			.list();
		Assert.state(!match.isEmpty(), "there is indeed a podcast with this id and this mogul");
	}

	@Override
	public Episode updatePodcastEpisodeDetails(Long episodeId, String title, String description, Date created) {
		Assert.notNull(episodeId, "the episode is null");
		title = StringUtils.hasText(title) ? title : "";
		description = StringUtils.hasText(description) ? description : "";
		this.db.sql("update podcast_episode set title = ?, description =? where id = ?")
			.params(title, description, episodeId)
			.update();
		// a null means "leave it alone", so an editor that doesn't offer the field can
		// keep calling this without flattening the date.
		if (null != created)
			this.db.sql("update podcast_episode set created = ? where id = ?").params(created, episodeId).update();
		this.invalidatePodcastEpisodeCache(episodeId);
		this.refreshPodcastEpisodeCompleteness(episodeId);
		var podcastEpisodeById = this.getPodcastEpisodeById(episodeId);
		this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(podcastEpisodeById));
		return podcastEpisodeById;
	}

	@Override
	public void writePodcastEpisodeProducedAudio(Long episodeId, Long managedFileId) {
		try {
			// the ManagedFile has already been reconciled with what the processors
			// module actually wrote; there is nothing left to move here, only a date to
			// record. this used to download the whole produced episode out of S3 and
			// push it straight back up again just to refresh one row.
			this.db //
				.sql("update podcast_episode set produced_audio_updated=? where id = ? ") //
				.params(new Date(), episodeId) //
				.update();
			this.invalidatePodcastEpisodeCache(episodeId);
			this.log.debug("updated episode {} to have non-null produced_audio_updated", episodeId);
			this.publisher.publishEvent(new PodcastEpisodeUpdatedEvent(getPodcastEpisodeById(episodeId)));
		} //
		catch (Throwable throwable) {
			throw new RuntimeException("got an exception dealing with " + throwable.getLocalizedMessage(), throwable);
		}
	}

	@Override
	public Collection<Episode> getAllPodcastEpisodesByIds(Collection<Long> episodeIds) {
		this.log.debug("getting episodes for episode ids(length {}) {}", episodeIds.size(), episodeIds);
		if (episodeIds.isEmpty()) {
			return Set.of();
		}
		var map = new HashMap<Long, Episode>();
		var idsNotInCache = CacheUtils.notPresentInCache(this.podcastEpisodesCache, episodeIds);
		if (!idsNotInCache.isEmpty()) {
			var idsArr = idsNotInCache.toArray(Long[]::new);
			var episodes = this.db //
				.sql("select * from podcast_episode pe where pe.id = any(? )") //
				.params(new SqlArrayValue("bigint", (Object[]) idsArr))
				.query(new EpisodeResultSetExtractor(true, managedFileService::getManagedFiles));
			for (var episode : episodes) {
				map.put(episode.id(), episode);
			}
		}
		var result = new ArrayList<Episode>();
		for (var id : episodeIds) {
			result.add(this.podcastEpisodesCache.get(id, () -> map.get(id)));
		}
		return result;
	}

	@Override
	public Collection<Podcast> getAllPodcastsByMogul(Long mogulId) {
		return this.db //
			.sql("select * from podcast p where p.mogul_id = ?")//
			.param(mogulId)//
			.query(this.podcastRowMapper)//
			.list();
	}

	@Override
	public Collection<Podcast> getAllPodcastsById(List<Long> mogulIds) {
		if (null == mogulIds || mogulIds.isEmpty())
			return Set.of();
		var idsArray = new Long[mogulIds.size()];
		for (var i = 0; i < mogulIds.size(); i++)
			idsArray[i] = mogulIds.get(i);
		return db//
			.sql("select * from podcast p where p.id = any(?)")//
			.params(new SqlArrayValue("bigint", (Object[]) idsArray))
			.query(this.podcastRowMapper)//
			.list();
	}

	static class SegmentResultSetExtractor implements ResultSetExtractor<List<Segment>> {

		private final Function<Collection<Long>, Map<Long, ManagedFile>> managedFileFunction;

		SegmentResultSetExtractor(Function<Collection<Long>, Map<Long, ManagedFile>> managedFileFunction) {
			this.managedFileFunction = managedFileFunction;
		}

		@Override
		public List<Segment> extractData(ResultSet rs) throws SQLException, DataAccessException {
			// every row first, then every managed file they mention in one lookup
			record Row(Long episodeId, Long id, Long audio, Long producedAudio, Long waveform, String edits,
					long crossFadeDuration, String name, int order, long duration) {
			}
			var rows = new ArrayList<Row>();
			var managedFileIds = new HashSet<Long>();
			while (rs.next()) {
				var waveform = rs.getObject("waveform_managed_file_id", Long.class);
				var row = new Row(rs.getLong("podcast_episode_id"), rs.getLong("id"),
						rs.getLong("segment_audio_managed_file_id"),
						rs.getLong("produced_segment_audio_managed_file_id"), waveform, rs.getString("edits"),
						rs.getLong("cross_fade_duration"), rs.getString("name"), rs.getInt("sequence_number"),
						rs.getLong("duration"));
				managedFileIds.add(row.audio());
				managedFileIds.add(row.producedAudio());
				if (waveform != null)
					managedFileIds.add(waveform);
				rows.add(row);
			}
			var managedFiles = this.managedFileFunction.apply(managedFileIds);
			var result = new ArrayList<Segment>();
			for (var row : rows)
				result.add(new Segment(row.episodeId(), row.id(), managedFiles.get(row.audio()),
						managedFiles.get(row.producedAudio()),
						row.waveform() == null ? null : managedFiles.get(row.waveform()),
						SegmentEdits.read(row.edits()), row.crossFadeDuration(), row.name(), row.order(),
						row.duration()));
			return result;
		}

	}

	/**
	 * reads every episode row first and then resolves all of their managed files in a
	 * single call, rather than three at a time per row. a hundred episodes is one lookup,
	 * not a hundred.
	 * <p>
	 * a shallow read skips the resolution entirely: the search results and the episode
	 * lists that only want titles have no use for the files, and asking for them is the
	 * expensive half of the job.
	 */
	static class EpisodeResultSetExtractor implements ResultSetExtractor<Collection<Episode>> {

		private final Function<Collection<Long>, Map<Long, ManagedFile>> managedFiles;

		private final boolean deep;

		EpisodeResultSetExtractor(boolean deep, Function<Collection<Long>, Map<Long, ManagedFile>> managedFiles) {
			this.managedFiles = managedFiles;
			this.deep = deep;
		}

		@Override
		public Collection<Episode> extractData(@NonNull ResultSet rs) throws SQLException, DataAccessException {
			var rows = new ArrayList<EpisodeRow>();
			var managedFileIds = new HashSet<Long>();
			while (rs.next()) {
				var row = new EpisodeRow(rs.getLong("id"), rs.getLong("podcast_id"), rs.getString("title"),
						rs.getString("description"), rs.getTimestamp("created"), rs.getLong("graphic_managed_file_id"),
						rs.getLong("produced_graphic_managed_file_id"), rs.getLong("produced_audio_managed_file_id"),
						rs.getBoolean("complete"), rs.getTimestamp("produced_audio_updated"),
						rs.getTimestamp("produced_audio_assets_updated"));
				rows.add(row);
				// a nullable produced_* column reads back as 0, and there is no row 0 to
				// go looking for.
				for (var id : List.of(row.graphicId(), row.producedGraphicId(), row.producedAudioId()))
					if (id > 0)
						managedFileIds.add(id);
			}
			var managedFileMap = this.deep ? this.managedFiles.apply(managedFileIds) : Map.<Long, ManagedFile>of();
			var results = new ArrayList<Episode>();
			for (var row : rows)
				results.add(new Episode(row.id(), row.podcastId(), row.title(), row.description(), row.created(),
						managedFileMap.get(row.graphicId()), managedFileMap.get(row.producedGraphicId()),
						managedFileMap.get(row.producedAudioId()), row.complete(), row.producedAudioUpdated(),
						row.producedAudioAssetsUpdated()));
			return results;
		}

		/**
		 * one row, read but not yet resolved: its three managed files are fetched for the
		 * whole batch once every row is in hand.
		 */
		private record EpisodeRow(Long id, Long podcastId, String title, String description, Date created,
				Long graphicId, Long producedGraphicId, Long producedAudioId, boolean complete,
				Date producedAudioUpdated, Date producedAudioAssetsUpdated) {
		}

	}

}
