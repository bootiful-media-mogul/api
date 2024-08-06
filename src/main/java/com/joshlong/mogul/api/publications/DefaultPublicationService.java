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
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.joshlong.mogul.api.PublisherPlugin.CONTEXT_URL;


@RegisterReflectionForBinding({ Publishable.class, PublisherPlugin.class })
class DefaultPublicationService implements PublicationService {

	public record PublicationStartedEvent(long publicationId) {
	}

	public record PublicationCompletedEvent(long publicationId) {
	}

	public record SettingsLookup(Long mogulId, String category) {
	}


	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Function<SettingsLookup, Map<String, String>> settingsLookup;

	private final JdbcClient db;

	private final MogulService mogulService;

	private final RowMapper<Publication> publicationRowMapper;

	private final TextEncryptor textEncryptor;

	private final TransactionTemplate transactionTemplate;

	DefaultPublicationService(JdbcClient db,
							  MogulService mogulService, TextEncryptor textEncryptor, TransactionTemplate tt,
							  Function<SettingsLookup, Map<String, String>> settingsLookup) {
		this.db = db;
		this.transactionTemplate = tt;
		this.settingsLookup = settingsLookup;
		this.mogulService = mogulService;
		this.textEncryptor = textEncryptor;
		this.publicationRowMapper = new PublicationRowMapper(textEncryptor);
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
			}

		} catch (Exception throwable) {
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

		var configuration = this.settingsLookup
				.apply(new SettingsLookup(this.mogulService.getCurrentMogul().id(), plugin.name()));
		var context = new ConcurrentHashMap<String, String>();
		context.putAll(configuration);
		context.putAll(contextAndSettings);

		var publicationId = this.transactionTemplate.execute(transactionStatus -> {
			var mogul = this.mogulService.getMogulById(mogulId);
			Assert.notNull(mogul, "the mogul should not be null");
			var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
			var publicationData = JsonUtils.write(payload.publicationKey());
			var entityClazz = payload.getClass().getName();
			var kh = new GeneratedKeyHolder();
			this.db.sql(
							"insert into publication( state,mogul_id, plugin, created, published, context, payload , payload_class) VALUES (?,?,?,?,?,?,?,?)")
					.params(Publication.State.DRAFT.name(), mogulId, plugin.name(), new Date(), null, contextJson,
							publicationData, entityClazz)
					.update(kh);
			return JdbcUtils.getIdFromKeyHolder(kh).longValue();
		});


		NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
				new PublicationStartedEvent(publicationId), Long.toString(publicationId), null, true, true));

		plugin.publish(context, payload);

		NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
				new PublicationCompletedEvent(publicationId), Long.toString(publicationId), null, true, true));

		this.log.debug("finished publishing with plugin {}.", plugin.name());

		var contextJsonAfterPublish = this.textEncryptor.encrypt(JsonUtils.write(context));
		this.db.sql(" update publication set state =? ,context = ?, published = ?  where id = ?")
				.params(Publication.State.PUBLISHED.name(), contextJsonAfterPublish, new Date(), publicationId)
				.update();

		var url = context.getOrDefault(CONTEXT_URL, null);
		if (null != url) {
			this.db.sql(" update publication set url = ? where id = ?").params(url, publicationId).update();
		}
		var publication = this.getPublicationById(publicationId);
		this.log.debug("writing publication out: {}", publication);
		return publication;
	}

	@Override
	public Publication getPublicationById(Long publicationId) {
		return this.db //
				.sql("select * from publication where id = ? ") //
				.params(publicationId)//
				.query(this.publicationRowMapper)//
				.single();
	}

	@Override
	public Collection<Publication> getPublicationsByPublicationKeyAndClass(Serializable publicationKey,
																		   Class<?> clazz) {
		var sql = " select * from publication where payload = ? and payload_class = ? ";
		var jsonPublicationKey = JsonUtils.write(publicationKey);
		return this.db//
				.sql(sql)//
				.params(jsonPublicationKey, clazz.getName())//
				.query(this.publicationRowMapper)//
				.stream() //
				.sorted(Comparator.comparing(Publication::created).reversed()) //
				.toList();
	}

}
