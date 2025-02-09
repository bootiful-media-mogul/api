package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublishableRepository;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.utils.JsonUtils;
import com.joshlong.mogul.api.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Controller
class PublicationController<T extends Publishable> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Settings settings;

	private final Map<String, Class<?>> publishableClasses = new ConcurrentHashMap<>();

	private final Executor executor = Executors.newVirtualThreadPerTaskExecutor();

	private final PublicationService publicationService;

	private final MogulService mogulService;

	private final Map<String, PublisherPlugin<T>> plugins = new ConcurrentHashMap<>();

	PublicationController(Settings settings, PublicationService publicationService, MogulService mogulService,
			Map<String, PublisherPlugin<?>> plugins, Map<String, PublishableRepository<?>> resolvers) {
		this.publicationService = publicationService;
		this.mogulService = mogulService;
		this.settings = settings;

		for (var p : plugins.entrySet()) {
			this.plugins.put(p.getKey(), (PublisherPlugin<T>) p.getValue());
		}

		for (var r : resolvers.entrySet()) {
			var resolver = r.getValue();
			for (var cl : ReflectionUtils.genericsFor(resolver.getClass())) {
				this.publishableClasses.put(cl.getSimpleName().toLowerCase(), cl);
			}
		}
	}

	// todo make publish and unpublish work.
	// todo remember to remove publish ad unpublush logic from the podcasts controller /
	// service.

	private Map<String, String> contextFromClient(String json) {
		if (StringUtils.hasText(json)) {
			// @formatter:off
            return JsonUtils.read(json, new ParameterizedTypeReference<>() {});
            // @formatter:on
		}
		return new HashMap<>();
	}

	@QueryMapping
	boolean canPublish(@Argument String publishableType, @Argument Serializable id, @Argument String contextJson,
			@Argument String plugin) {

		var context = this.contextFromClient(contextJson);
		var aClass = resolve(publishableType);
		if (id instanceof Number idAsNumber) {
			var mogulId = this.mogulService.getCurrentMogul().id();
			var publishable = this.publicationService.resolvePublishable(mogulId, idAsNumber.longValue(), aClass);
			Assert.state(this.plugins.containsKey(plugin), "the plugin named [" + plugin + "] does not exist!");
			var resolvedPlugin = this.plugins.get(plugin);
			var configuration = this.settings.getAllValuesByCategory(mogulService.getCurrentMogul().id(), plugin);

			// do NOT allow client side overrides of settings
			var combinedContext = new HashMap<String, String>(configuration);
			for (var k : context.keySet()) {
				if (!combinedContext.containsKey(k)) {
					combinedContext.put(k, context.get(k));
				} //
				else {
					this.log.warn("refusing to add client specified context value '{}',"
							+ "because it would override a user specified setting", k);
				}
			}
			return resolvedPlugin.canPublish(combinedContext, publishable);
		}
		throw new IllegalStateException(
				"we should never arrive at this point, " + "but if we did it's because we can't find a "
						+ Publishable.class.getName() + " of type " + aClass.getName());
	}

	@SuppressWarnings("unchecked")
	private Class<T> resolve(String type) {
		var match = (Class<T>) this.publishableClasses.getOrDefault(type.toLowerCase(), null);
		Assert.notNull(match, "couldn't find a matching class for type [" + type + "]");
		return match;
	}

	@MutationMapping
	boolean publish(@Argument String publishableType, @Argument Serializable id, @Argument String contextJson,
			@Argument String plugin) {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var aClass = resolve(publishableType);
		if (id instanceof Number idAsNumber) {
			var mogulId = this.mogulService.getCurrentMogul().id();
			var episode = this.publicationService.resolvePublishable(mogulId, idAsNumber.longValue(), aClass);
			var auth = SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication();
			var runnable = (Runnable) () -> {
				SecurityContextHolder.getContext().setAuthentication(auth);
				this.mogulService.assertAuthorizedMogul(currentMogulId);
				var contextAndSettings = this.contextFromClient(contextJson);
				this.publicationService.publish(currentMogulId, episode, contextAndSettings, this.plugins.get(plugin));
			};
			this.executor.execute(runnable);
			return true;
		}
		return false;
	}

	// todo tie the UNpublication logic into this new subsystem!
	@MutationMapping
	boolean unpublish(@Argument Long publicationId) {
		var runnable = (Runnable) () -> {
			this.log.debug("going to unpublish the publication with id # {}", publicationId);
			var publicationById = this.publicationService.getPublicationById(publicationId);
			Assert.notNull(publicationById, "the publication should not be null");
			var resolvedPlugin = this.plugins.get(publicationById.plugin());
			Assert.notNull(resolvedPlugin, "you must specify an active plugin");
			this.publicationService.unpublish(publicationById.mogulId(), publicationById, resolvedPlugin);
		};
		this.executor.execute(runnable);
		return true;
	}

}
/*
 *
 *
 * @MutationMapping boolean unpublishPodcastEpisodePublication(@Argument Long
 * publicationId) { var runnable = (Runnable) () -> {
 * log.debug("going to unpublish the publication with id # {}", publicationId); var
 * publicationById = this.publicationService.getPublicationById(publicationId);
 * Assert.notNull(publicationById, "the publication should not be null"); var plugin =
 * this.plugins.get(publicationById.plugin()); Assert.notNull(plugin,
 * "you must specify an active plugin");
 * this.publicationService.unpublish(publicationById.mogulId(), publicationById, plugin);
 * }; this.executor.execute(runnable); return true; }
 */
