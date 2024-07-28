package com.joshlong.mogul.api.publications;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.utils.JdbcUtils;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Configuration
@RegisterReflectionForBinding(com.joshlong.mogul.api.podcasts.Episode.class)
class DefaultPublicationServiceConfiguration {

	@Bean
	DefaultPublicationService defaultPublicationService(JdbcClient client, MogulService mogulService,
			TextEncryptor textEncryptor, Settings settings, Map<String, PublisherPlugin<?>> plugins,
			ObjectMapper objectMapper) {
		var lookup = new SettingsLookupClient(settings);
		return new DefaultPublicationService(client, mogulService, textEncryptor, lookup, plugins, objectMapper);
	}

}

// todo remove the word public here. it's only being used to support the one time
// migration
@Transactional
@RegisterReflectionForBinding({ Publishable.class, PublisherPlugin.class })
public class DefaultPublicationService implements PublicationService {

	public record SettingsLookup(Long mogulId, String category) {
	}

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<String, PublisherPlugin<?>> plugins = new ConcurrentHashMap<>();

	private final Function<SettingsLookup, Map<String, String>> settingsLookup;

	private final JdbcClient db;

	private final MogulService mogulService;

	private final RowMapper<Publication> publicationRowMapper;

	private final TextEncryptor textEncryptor;

	// todo remove the word public
	public DefaultPublicationService(JdbcClient db, MogulService mogulService, TextEncryptor textEncryptor,
			Function<SettingsLookup, Map<String, String>> settingsLookup, Map<String, PublisherPlugin<?>> plugins,
			ObjectMapper objectMapper) {
		this.db = db;
		this.settingsLookup = settingsLookup;
		this.mogulService = mogulService;
		this.textEncryptor = textEncryptor;
		this.plugins.putAll(plugins);
		this.publicationRowMapper = new PublicationRowMapper(objectMapper, textEncryptor);
		Assert.notNull(this.db, "the JdbcClient must not be null");
		Assert.notNull(this.mogulService, "the mogulService must not be null");
		Assert.notNull(this.textEncryptor, "the textEncryptor must not be null");
		Assert.notNull(this.settingsLookup, "the settings must not be null");
		Assert.state(!this.plugins.isEmpty(), "there are no plugins for publication");
	}

	@Override
	public <T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin) {
		var mogul = this.mogulService.getMogulById(mogulId);
		Assert.notNull(plugin, "the plugin must not be null");
		Assert.notNull(payload, "the payload must not be null");
		Assert.notNull(mogul, "the mogul should not be null");
		var configuration = this.settingsLookup
			.apply(new SettingsLookup(this.mogulService.getCurrentMogul().id(), plugin.name()));
		var context = new ConcurrentHashMap<String, String>();
		context.putAll(configuration);
		context.putAll(contextAndSettings);
		plugin.publish(context, payload);
		log.debug("finished publishing with plugin {}.", plugin.name());
		var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
		var publicationData = this.textEncryptor.encrypt(JsonUtils.write(payload.publicationKey()));
		var entityClazz = payload.getClass().getName();
		var kh = new GeneratedKeyHolder();
		this.db.sql(
				"insert into publication(mogul_id, plugin, created, published, context, payload , payload_class) VALUES (?,?,?,?,?,?,?)")
			.params(mogulId, plugin.name(), new Date(), null, contextJson, publicationData, entityClazz)
			.update(kh);
		var publication = this.getPublicationById(JdbcUtils.getIdFromKeyHolder(kh).longValue());
		log.debug("writing publication out: {}", publication);
		return publication;
	}

	@Override
	public Publication getPublicationById(Long publicationId) {
		return this.db.sql("select * from publication where id =? ")
			.params(publicationId)
			.query(this.publicationRowMapper)
			.single();
	}

}
