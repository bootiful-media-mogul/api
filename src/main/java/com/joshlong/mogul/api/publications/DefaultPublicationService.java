package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.*;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.utils.JdbcUtils;
import com.joshlong.mogul.utils.JsonUtils;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.SqlArrayValue;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
@RegisterReflectionForBinding({ Publishable.class, PublisherPlugin.class, PublisherPlugin.PublishContext.class,
		PublisherPlugin.UnpublishContext.class, PublisherPlugin.Context.class, PublishRequest.class })
class DefaultPublicationService extends AbstractDomainService<Publishable, PublishableResolver<?>>
		implements PublicationService {

	// @formatter:off
	private static final ParameterizedTypeReference<Map<String, String>> AUTHENTICATION_DETAILS_TYPE =
			new ParameterizedTypeReference<>() {};
	// @formatter:on

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Function<SettingsLookup, Map<String, String>> settingsLookup;

	private final JdbcClient db;

	private final MogulService mogulService;

	private final TextEncryptor textEncryptor;

	private final TransactionTemplate transactionTemplate;

	private final ApplicationEventPublisher publisher;

	/**
	 * looked up when it's first needed: jobrunr's storage needs every handler before the
	 * scheduler can exist, and {@link PublishRequestHandler} needs this.
	 */
	private final ObjectProvider<JobRequestScheduler> jobs;

	DefaultPublicationService(JdbcClient db, MogulService mogulService, TextEncryptor textEncryptor,
			TransactionTemplate tt, Function<SettingsLookup, Map<String, String>> settingsLookup,
			Map<String, PublishableResolver<?>> resolvers, ApplicationEventPublisher publisher,
			ObjectProvider<JobRequestScheduler> jobs) {
		super(resolvers.values());
		this.jobs = jobs;
		this.db = db;
		this.transactionTemplate = tt;
		this.settingsLookup = settingsLookup;
		this.mogulService = mogulService;
		this.textEncryptor = textEncryptor;
		this.publisher = publisher;
	}

	/*
	 * do <EM>NOT</EM> make this a shared class variable. There's <EM>state</EM> in the
	 * {@link PublicationRowMapper}, and you'll see duplicate records if this is used
	 * across more than one request
	 */
	private PublicationRowMapper getPublicationRowMapper() {
		return new PublicationRowMapper(this.db, this.textEncryptor);
	}

	@Override
	public <T extends Publishable> T resolvePublishable(Long mogulId, Long id, String clazz) {
		return this.resolvePublishable(mogulId, id, this.classForType(clazz));
	}

	@Override
	public <T extends Publishable> T resolvePublishable(Long mogulId, Long id, Class<T> clazz) {
		this.mogulService.assertAuthorizedMogul(mogulId);
		return findEntity(clazz, id);
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
			var uc = new PublisherPlugin.UnpublishContext<T>(newContext, publication);
			if (plugin.unpublish(uc)) {
				var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
				this.db.sql("update publication set state = ?, context = ? where id =? ")
					.params(Publication.State.UNPUBLISHED.name(), contextJson, publication.id())
					.update();
				this.publisher.publishEvent(new PublicationUpdatedEvent(publication));
			}
		} //
		catch (Exception throwable) {
			this.log.warn("couldn't unpublish {} ", publication.id(), throwable);
		}
		return this.getPublicationById(publication.id());

	}

	@Override
	public <T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin, @Nullable Instant when) {
		Assert.notNull(plugin, "the plugin must not be null");
		Assert.notNull(payload, "the payload must not be null");
		this.mogulService.assertAuthorizedMogul(mogulId);
		var configuration = this.settingsLookup.apply(new SettingsLookup(mogulId, plugin.name()));
		var context = new HashMap<String, String>();
		context.putAll(configuration);
		context.putAll(contextAndSettings);
		context.put(MOGUL_ID, Long.toString(mogulId));
		var details = this.currentAuthenticationDetails();
		if (!details.isEmpty())
			context.put(AUTHENTICATION_DETAILS, JsonUtils.write(details));
		var publicationId = (long) Objects.requireNonNull(this.transactionTemplate.execute(_ -> {
			var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
			var publicationData = JsonUtils.write(payload.publishableId());
			var entityClazz = payload.getClass().getName();
			var kh = new GeneratedKeyHolder();
			this.db
				.sql("""
						insert into publication( state, mogul_id, plugin, created, published, context, payload, payload_class, scheduled )
						values (?,?,?,?,?,?,?,?,?)
						""")
				.params(Publication.State.PENDING.name(), mogulId, plugin.name(), new Date(), null, contextJson,
						publicationData, entityClazz, when == null ? null : Timestamp.from(when))
				.update(kh);
			return JdbcUtils.getIdFromKeyHolder(kh).longValue();
		}));
		// before the job exists, so a job quick enough to finish first can't have its
		// completion overtaken by the news that it started
		this.doNotify(mogulId, publicationId, new PublicationStartedEvent(this.getPublicationById(publicationId)));
		// only once the row is committed: a job that runs before then finds nothing to do
		var request = new PublishRequest(publicationId, mogulId, false);
		try {
			var jobs = this.jobs.getObject();
			var jobId = (when == null || !when.isAfter(Instant.now())) ? jobs.enqueue(request)
					: jobs.schedule(when, request);
			this.log.debug("publication {} for mogul #{} is jobrunr job {}, to run at {}", publicationId, mogulId,
					jobId, when == null ? "once a worker is free" : when);
		} //
		catch (RuntimeException e) {
			// a publication that never reaches jobrunr would stay pending forever
			this.log.warn("couldn't hand publication {} to jobrunr", publicationId, e);
			this.fail(this.getPublicationById(publicationId), plugin.name(),
					"couldn't schedule the publication: " + e.getMessage());
		}
		return this.getPublicationById(publicationId);
	}

	/**
	 * rebuilds what's needed to run a pending publication from its row: the payload as it
	 * is <em>now</em>, and the context it was asked for with, under the plugin's current
	 * settings -- a scheduled publication may run long after a token in it was rotated.
	 * like the settings page, a setting wins over a client-supplied value of the same
	 * name. call this as the mogul; see {@link #runAs}.
	 */
	<T extends Publishable> PublicationAttempt<T> attemptFor(Publication publication, PublisherPlugin<T> plugin) {
		var mogulId = publication.mogulId();
		var payload = this.<T>resolvePublishable(mogulId, Long.parseLong(publication.payload()),
				(Class<T>) publication.payloadClass());
		Assert.notNull(payload, "the " + publication.payloadClass().getSimpleName() + " #" + publication.payload()
				+ " for publication " + publication.id() + " no longer exists");
		var context = new ConcurrentHashMap<String, String>(publication.context());
		context.putAll(this.settingsLookup.apply(new SettingsLookup(mogulId, plugin.name())));
		context.put(MOGUL_ID, Long.toString(mogulId));
		context.put(PUBLICATION_ID, Long.toString(publication.id()));
		return new PublicationAttempt<>(publication.id(), mogulId, plugin,
				PublisherPlugin.PublishContext.of(mogulId, payload, context));
	}

	/**
	 * runs {@code work} authenticated as the publication's mogul, the way the request
	 * that asked for it was: plugins look up the current mogul, and some of them a token
	 * the gateway added to the authentication's details. jobrunr's threads are pooled, so
	 * whatever was there before is put back afterwards.
	 */
	void runAs(Publication publication, Runnable work) {
		var mogul = this.mogulService.getMogulById(publication.mogulId());
		Assert.notNull(mogul, "there is no mogul #" + publication.mogulId());
		var authentication = UsernamePasswordAuthenticationToken.authenticated(mogul.username(), null, List.of());
		var details = publication.context().get(AUTHENTICATION_DETAILS);
		if (StringUtils.hasText(details))
			authentication.setDetails(new HashMap<>(JsonUtils.read(details, AUTHENTICATION_DETAILS_TYPE)));
		var strategy = SecurityContextHolder.getContextHolderStrategy();
		var previous = strategy.getContext();
		var context = strategy.createEmptyContext();
		context.setAuthentication(authentication);
		strategy.setContext(context);
		try {
			work.run();
		} //
		finally {
			strategy.setContext(previous);
		}
	}

	private Map<String, String> currentAuthenticationDetails() {
		var details = new HashMap<String, String>();
		var authentication = SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication();
		if (authentication != null && authentication.getDetails() instanceof Map<?, ?> map)
			map.forEach((k, v) -> {
				if (v instanceof String value)
					details.put(String.valueOf(k), value);
			});
		return details;
	}

	/**
	 * finishes a publication that never got as far as an attempt -- its payload is gone,
	 * say, or its plugin -- so the client stops waiting on it.
	 */
	Publication fail(Publication publication, String outcomeKey, String error) {
		this.log.warn("abandoning publication {}: {}", publication.id(), error);
		var pc = PublisherPlugin.PublishContext.of(publication.mogulId(), null, new HashMap<>(publication.context()));
		pc.failure(outcomeKey, error);
		return this.record(publication.id(), publication.mogulId(), pc);
	}

	@Override
	public <T extends Publishable> Publication completePublication(PublicationAttempt<T> attempt) {
		Assert.notNull(attempt, "the attempt must not be null");
		var plugin = attempt.plugin();
		var pc = attempt.publishContext();
		try {
			plugin.publish(pc);
		} //
		catch (Throwable throwable) {
			pc.failure(plugin.name(), throwable.getMessage());
			this.log.warn("couldn't publish {} ", attempt.publicationId(), throwable);
		}
		return this.record(attempt);
	}

	@Override
	public <T extends Publishable> Publication failPublication(PublicationAttempt<T> attempt, String error) {
		Assert.notNull(attempt, "the attempt must not be null");
		this.log.warn("abandoning publication {}: {}", attempt.publicationId(), error);
		attempt.publishContext().failure(attempt.plugin().name(), error);
		return this.record(attempt);
	}

	/**
	 * writes down how it went. the publication is marked finished either way -- an
	 * attempt that stays {@code PENDING} is one the client never stops waiting on.
	 */
	private <T extends Publishable> Publication record(PublicationAttempt<T> attempt) {
		return this.record(attempt.publicationId(), attempt.mogulId(), attempt.publishContext());
	}

	private Publication record(Long publicationId, Long mogulId, PublisherPlugin.PublishContext<?> pc) {
		var context = pc.context();

		return this.transactionTemplate.execute(_ -> {

			this.db.sql(" update publication set state = ? , context = ?, published = ?  where id = ?")
				.params(Publication.State.PUBLISHED.name(), textEncryptor.encrypt(JsonUtils.write(context)), new Date(),
						publicationId)
				.update();
			pc.outcomes().forEach((outcome) -> {
				this.db.sql(
						"insert into publication_outcome(publication_id, success, uri , key ,server_error_message, preview ) values (?,?,?,?,?,?)")
					.params(publicationId, outcome.success(), outcome.uri() != null ? outcome.uri().toString() : null,
							outcome.key(), outcome.serverErrorMessage(), outcome.preview())
					.update();
			});
			this.doNotify(mogulId, publicationId,
					new PublicationCompletedEvent(this.getPublicationById(publicationId)));
			return this.getPublicationById(publicationId);
		});
	}

	private void doNotify(Long mogulId, Long publicationId, Object event) {
		this.publisher.publishEvent(event);
		NotificationEvents.notifyAsync(
				NotificationEvent.systemNotificationEventFor(mogulId, event, Long.toString(publicationId), "{}"));
	}

	@Override
	public Publication getPublicationById(Long publicationId) {
		var all = this.db //
			.sql("select * from publication where id = ?") //
			.param(publicationId) //
			.query(this.getPublicationRowMapper()) //
			.list();
		if (all.isEmpty()) {
			return null;
		}
		return all.getFirst();

	}

	@Override
	public Map<Long, Publication> getPublicationsByIds(Collection<Long> badIds) {
		var ids = badIds.stream().filter(id -> id > 0).collect(Collectors.toSet());
		if (ids.isEmpty() || ids.stream().noneMatch(id -> id > 0))
			return Collections.emptyMap();
		var map = new HashMap<Long, Publication>();
		var pubs = this.db //
			.sql("select * from publication p where p.id = any(?) ") //
			.params(new SqlArrayValue("bigint", ids.toArray())) //
			.query(this.getPublicationRowMapper()) //
			.list();
		for (var p : pubs)
			map.put(p.id(), p);

		return map;
	}

	@Override
	public Collection<Publication> getPublicationsByPublicationKeyAndClass(Long publicationKey, Class<?> clazz) {
		var publications = this.db //
			.sql("select * from publication where payload = ? and payload_class = ? order by created desc") //
			.params(Long.toString(publicationKey), clazz.getName())//
			.query(this.getPublicationRowMapper()) //
			.list();
		for (var p : publications)
			this.log.debug("found publication {} with created {} and published {}", p, p.created(), p.published());
		return publications;
	}

	@Override
	public Collection<Publication> getPublicationsByPublicationKeyAndClass(Long publicationKey, String clazz) {
		return this.getPublicationsByPublicationKeyAndClass(publicationKey, this.classForType(clazz));
	}

	@Override
	public Map<Long, Collection<Publication>> getPublicationsByPublicationKeysAndClass(Collection<Long> publicationKeys,
			Class<?> clazz) {
		var keys = new LinkedHashSet<>(publicationKeys);
		var results = new LinkedHashMap<Long, Collection<Publication>>();
		for (var key : keys)
			results.put(key, new ArrayList<>());
		if (keys.isEmpty())
			return results;
		var payloads = keys.stream().map(key -> Long.toString(key)).toArray(String[]::new);
		var publications = this.db //
			.sql("select * from publication where payload = any(?) and payload_class = ? order by created desc") //
			.params(new SqlArrayValue("text", payloads), clazz.getName()) //
			.query(this.getPublicationRowMapper()) //
			.list();
		for (var publication : publications)
			results.computeIfAbsent(Long.parseLong(publication.payload()), _ -> new ArrayList<>()).add(publication);
		return results;
	}

	public record SettingsLookup(Long mogulId, String category) {

	}

}
