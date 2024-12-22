package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.utils.JdbcUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.joshlong.mogul.api.PublisherPlugin.CONTEXT_URL;

@RegisterReflectionForBinding({ Publishable.class, PublisherPlugin.class })
class DefaultPublicationService implements PublicationService {

	public record SettingsLookup(Long mogulId, String category) {

	}

	private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

	private final Logger log = LoggerFactory.getLogger(getClass());

	// mapping of an element (podcast episode, youtube video, blog, etc.)'s id to the
	// publications associated with that element
	private final Map<String, Collection<Publication>> publicationsCache = new ConcurrentHashMap<>();

	private final Function<SettingsLookup, Map<String, String>> settingsLookup;

	private final JdbcClient db;

	private final MogulService mogulService;

	private final RowMapper<Publication> publicationRowMapper;

	private final TextEncryptor textEncryptor;

	private final TransactionTemplate transactionTemplate;

	private final ApplicationEventPublisher publisher;

	DefaultPublicationService(JdbcClient db, MogulService mogulService, TextEncryptor textEncryptor,
			TransactionTemplate tt, Function<SettingsLookup, Map<String, String>> settingsLookup,
			ApplicationEventPublisher publisher) {
		this.db = db;
		this.transactionTemplate = tt;
		this.settingsLookup = settingsLookup;
		this.mogulService = mogulService;
		this.textEncryptor = textEncryptor;
		this.publicationRowMapper = new PublicationRowMapper(textEncryptor);
		this.publisher = publisher;
	}

	@Override
	public <T extends Publishable> Publication unpublish(Long mogulId, Publication publication,
			PublisherPlugin<T> plugin) {
		var mogul = this.mogulService.getMogulById(mogulId);
		Assert.notNull(plugin, "the plugin must not be null");
		Assert.notNull(publication, "the publication must not be null");
		Assert.notNull(mogul, "the mogul should not be null");

		var context = publication.context();
		var newContext = new HashMap<>(context);

		try {
			if (plugin.unpublish(newContext, publication)) {
				var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
				this.db.sql("update publication set state = ?, context = ? where id =? ")
					.params(Publication.State.UNPUBLISHED.name(), contextJson, publication.id())
					.update();

				this.publisher.publishEvent(new PublicationUpdatedEvent(publication.id()));
			}
		}
		catch (Exception throwable) {
			log.warn("couldn't unpublish {} with url {}", publication.id(), publication.url());
			//
		}
		return this.getPublicationById(publication.id());
	}
	// do this on a separate thread so that it's
	// not included in the longer running parent transaction

	@Override
	public <T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin) {
		Assert.notNull(plugin, "the plugin must not be null");
		Assert.notNull(payload, "the payload must not be null");
		var configuration = this.settingsLookup.apply(new SettingsLookup(mogulId, plugin.name()));
		var context = new ConcurrentHashMap<String, String>();
		context.putAll(configuration);
		context.putAll(contextAndSettings);
		var publicationId = (long) Objects.requireNonNull(this.transactionTemplate.execute(transactionStatus -> {
			var mogul = this.mogulService.getMogulById(mogulId);
			Assert.notNull(mogul, "the mogul should not be null");
			var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
			var publicationData = JsonUtils.write(payload.publicationKey());
			var entityClazz = payload.getClass().getName();
			var kh = new GeneratedKeyHolder();
			this.db.sql(
					"insert into publication( state, mogul, plugin, created, published, context, payload , payload_class ) VALUES (?,?,?,?,?,?,?,?)")
				.params(Publication.State.DRAFT.name(), mogulId, plugin.name(), new Date(), null, contextJson,
						publicationData, entityClazz)
				.update(kh);
			return JdbcUtils.getIdFromKeyHolder(kh).longValue();
		}));

		// make sure the client sees the new state immediately.
		this.publisher.publishEvent(new PublicationUpdatedEvent(publicationId));

		NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
				new PublicationStartedEvent(publicationId), Long.toString(publicationId), null, true, true));

		plugin.publish(context, payload);

		this.publisher.publishEvent(new PublicationUpdatedEvent(publicationId));

		NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
				new PublicationCompletedEvent(publicationId), Long.toString(publicationId), null, true, true));

		this.log.debug("finished publishing with plugin {}.", plugin.name());

		var contextJsonAfterPublish = this.textEncryptor.encrypt(JsonUtils.write(context));
		this.db.sql(" update publication set state = ? , context = ?, published = ? where id = ?")
			.params(Publication.State.PUBLISHED.name(), contextJsonAfterPublish, new Date(), publicationId)
			.update();

		var url = context.getOrDefault(CONTEXT_URL, null);
		if (null != url) {
			this.db.sql(" update publication set url = ? where id = ? ").params(url, publicationId).update();
		}

		this.publisher.publishEvent(new PublicationUpdatedEvent(publicationId));

		var publication = this.getPublicationById(publicationId);
		this.log.debug("writing publication out: {}", publication);
		return publication;
	}

	@Override
	public Publication getPublicationById(Long publicationId) {

		for (var pc : this.publicationsCache.values())
			for (var p : pc)
				if (p.id().equals(publicationId))
					return p;

		throw new IllegalStateException(
				"we shouldn't get to this point. what are you doing looking up a publication whose ID (" + publicationId
						+ ") doesn't match? ");
	}

	private void refreshCache() {

		this.publicationsCache.clear();

		this.db.sql("select * from publication") //
			.query(this.publicationRowMapper) //
			.stream()//
			.forEach(publication -> {
				var key = this.buildPublicationCacheKey(publication.payloadClass(), publication.payload());
				this.publicationsCache.computeIfAbsent(key, cacheKey -> new ArrayList<>()).add(publication);
				this.log.debug("adding publication {} to publications cache for cache key {} ", publication, key);
			});
	}

	private String buildPublicationCacheKey(Class<?> c, Serializable s) {
		return c.getName() + ":" + s.toString();
	}

	@Override
	public Collection<Publication> getPublicationsByPublicationKeyAndClass(Serializable publicationKey,
			Class<?> clazz) {
		var key = this.buildPublicationCacheKey(clazz, publicationKey);
		if (this.publicationsCache.containsKey(key)) {
			return this.publicationsCache//
				.get(key)//
				.stream()//
				.sorted(Comparator.comparing(Publication::created).reversed())//
				.toList();
		}
		return List.of();
	}

	@EventListener
	void applicationReady(ApplicationReadyEvent are) {
		this.scheduledExecutorService.submit(this::refreshCache);
		// todo why is this a minute later? why did this code exist?
		// this.scheduledExecutorService.schedule(this::refreshCache, 1, TimeUnit.);
	}

	@EventListener(PublicationUpdatedEvent.class)
	void updated() {
		this.refreshCache();
	}

	@EventListener(PublicationStartedEvent.class)
	void started() {
		this.refreshCache();
	}

	@EventListener(PublicationCompletedEvent.class)
	void completed() {
		this.refreshCache();
	}

}
