package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.settings.Settings;
import com.joshlong.mogul.utils.DateUtils;
import com.joshlong.mogul.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.graphql.data.method.annotation.*;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Controller
class PublicationController<T extends Publishable> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Settings settings;

	private final PublicationService publicationService;

	private final MogulService mogulService;

	private final Map<String, PublisherPlugin<T>> plugins = new ConcurrentHashMap<>();

	PublicationController(Settings settings, PublicationService publicationService, MogulService mogulService,
			Map<String, PublisherPlugin<?>> plugins) {
		this.publicationService = publicationService;
		this.mogulService = mogulService;
		this.settings = settings;
		plugins.forEach((k, v) -> this.plugins.put(k, (PublisherPlugin<T>) v));
	}

	@QueryMapping
	boolean canPublish(@Argument Long publishableId, @Argument String publishableType, @Argument String contextJson,
			@Argument String plugin) {
		var context = this.contextFromClient(contextJson);
		var mogulId = mogulService.getCurrentMogul().id();
		var publishable = (T) this.publicationService.resolvePublishable(mogulId, publishableId, publishableType);
		Assert.state(this.plugins.containsKey(plugin), "the plugin named [" + plugin + "] does not exist!");
		var resolvedPlugin = this.plugins.get(plugin);
		var configuration = this.settings.getAllValuesByCategory(mogulId, plugin);
		var combinedContext = new HashMap<>(configuration);
		for (var k : context.keySet()) {
			if (!combinedContext.containsKey(k)) {
				combinedContext.put(k, context.get(k));
			} //
			else {
				this.log.warn("refusing to add client specified context value '{}',"
						+ "because it would override a user specified setting", k);
			}
		}
		var pc = PublisherPlugin.PublishContext.of(mogulId, publishable, combinedContext);
		return resolvedPlugin.canPublish(pc);
	}

	/**
	 * asks for a publication, now or at {@code scheduled}, and returns its id straight
	 * away. nothing has been published by then -- the publication is pending, and the
	 * client hears how it went from the notifications that follow, the same way whether
	 * it was for right now or for next week.
	 */
	@MutationMapping
	Long publish(@Argument Long publishableId, @Argument String publishableType, @Argument String contextJson,
			@Argument String plugin, @Argument OffsetDateTime scheduled,
			@ContextValue(name = PublicationService.BASE_URL, required = false) String baseUrl) {
		this.log.debug(
				"going to publish the publication with id # {} and type # {} and context JSON :: {} :: and plugin named {} at {}",
				publishableId, publishableType, contextJson, plugin, scheduled);
		Assert.hasText(plugin, "the plugin named [" + plugin + "] does not exist!");
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var publishable = (T) this.publicationService.resolvePublishable(currentMogulId, publishableId,
				publishableType);
		Assert.state(this.plugins.containsKey(plugin), "the plugin named [" + plugin + "] does not exist!");
		var publisherPlugin = this.plugins.get(plugin);
		var contextAndSettings = this.contextFromClient(contextJson);
		if (StringUtils.hasText(baseUrl))
			contextAndSettings.put(PublicationService.BASE_URL, baseUrl);
		var publication = this.publicationService.publish(currentMogulId, publishable, contextAndSettings,
				publisherPlugin, scheduled == null ? null : scheduled.toInstant());
		return publication.id();
	}

	@QueryMapping
	Collection<Publication> publicationsForPublishable(@Argument String type, @Argument Long id) {
		return this.publicationService.getPublicationsByPublicationKeyAndClass(id, type);
	}

	@SchemaMapping
	String url(Publication publication) {
		if (publication.outcomes() != null && !publication.outcomes().isEmpty()) {
			var uri = publication.outcomes().getFirst().url();
			if (null == uri)
				return null;
			return uri.toString();
		}
		return null;
	}

	@SchemaMapping
	OffsetDateTime published(Publication publication) {
		return DateUtils.forDate(publication.published());
	}

	@SchemaMapping
	OffsetDateTime scheduled(Publication publication) {
		return DateUtils.forDate(publication.scheduled());
	}

	@SchemaMapping
	OffsetDateTime created(Publication publication) {
		return DateUtils.forDate(publication.created());
	}

	@SchemaMapping(typeName = "PublicationOutcome", field = "created")
	OffsetDateTime outcomeCreated(Publication.Outcome outcome) {
		return DateUtils.forDate(outcome.created());
	}

	@SchemaMapping
	String state(Publication publication) {
		return publication.state().name().toLowerCase();
	}

	@MutationMapping
	boolean unpublish(@Argument Long publicationId) {
		this.log.debug("going to unpublish the publication with id # {}", publicationId);
		var publicationById = this.publicationService.getPublicationById(publicationId);
		Assert.notNull(publicationById, "the publication should not be null");
		var resolvedPlugin = this.plugins.get(publicationById.plugin());
		Assert.notNull(resolvedPlugin, "you must specify an active plugin");
		this.publicationService.unpublish(publicationById.mogulId(), publicationById, resolvedPlugin);
		return true;
	}

	private Map<String, String> contextFromClient(String json) {
		if (StringUtils.hasText(json)) {
			// @formatter:off
			var data = JsonUtils.read(json, new ParameterizedTypeReference<Map <String, Object>>() {});
            // @formatter:on
			var newMap = new HashMap<String, String>();
			for (var k : data.keySet()) {
				newMap.put(k, data.get(k).toString());
			}
			return newMap;

		}
		return new HashMap<>();
	}

}
