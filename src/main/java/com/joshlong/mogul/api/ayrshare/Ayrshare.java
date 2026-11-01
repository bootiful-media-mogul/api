package com.joshlong.mogul.api.ayrshare;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

@ImportRuntimeHints(Ayrshare.Hints.class)
class Ayrshare {

	private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

	private final RestClient http;

	private final Logger log = LoggerFactory.getLogger(getClass());

	Ayrshare(String apiKey) {
		this.http = RestClient.builder() //
			.baseUrl("https://api.ayrshare.com/api/") //
			.configureMessageConverters(clientBuilder -> clientBuilder.registerDefaults()
				.withJsonConverter(new JacksonJsonHttpMessageConverter()))
			.defaultHeaders(h -> {
				h.setBearerAuth(apiKey); //
				h.setContentType(MediaType.APPLICATION_JSON); //
			}) //
			.build();

	}

	private static Response.Status from(String string) {
		if (string != null) {
			if (string.equalsIgnoreCase("success")) {
				return Response.Status.SUCCESS;
			}
			if (string.equalsIgnoreCase("scheduled")) {
				return Response.Status.SCHEDULED;
			}
		}
		return Response.Status.ERRORS;
	}

	private static <T> T nullOrNode(JsonNode node, String name, Function<JsonNode, T> function) {
		Assert.notNull(node, "the node against which to evaluate tests should not be null");
		var jsonNode = node.get(name);
		if (jsonNode == null || jsonNode.isNull()) {
			return null;
		}
		Assert.notNull(function, "the mapping function should not be null");
		return function.apply(jsonNode);
	}

	Response post(String post, Platform[] platforms, Consumer<PostContext> contextConsumer) {
		var ctx = new PostContext();
		if (contextConsumer != null)
			contextConsumer.accept(ctx);
		return this.doPost(ctx.idempotencyKey.get(), post, platforms, ctx.mediaUris.toArray(new URI[0]),
				ctx.scheduledDate.get(), ctx.customHeaders);
	}

	private Response doPost(String idempotencyKey, String post, Platform[] platforms, URI[] mediaUris,
			Instant scheduledDate, final Map<String, String> headers) {
		Assert.hasText(post, "the post should not be empty!");
		Assert.state(platforms.length > 0, "there should be at least one platform specified!");

		if (mediaUris == null)
			mediaUris = new URI[0];

		var body = new HashMap<String, Object>();
		body.put("post", post);

		if (StringUtils.hasText(idempotencyKey)) {
			body.put("idempotencyKey", idempotencyKey);
		}

		var platformsArray = Stream.of(platforms).map(Platform::platformCode).toArray(String[]::new);
		body.put("platforms", platformsArray);
		if (scheduledDate != null) {
			body.put("scheduleDate", ISO_INSTANT.format(scheduledDate));
		}

		var mediaUrlsArray = Stream.of(mediaUris).map(URI::toString).toArray(String[]::new);
		if (mediaUrlsArray.length > 0) {
			body.put("mediaUrls", mediaUrlsArray);
		}
		var json = this.http.post()
			.uri("/post") //
			.headers(http -> {
				for (var customHeader : headers.entrySet()) {
					if (this.log.isInfoEnabled()) {
						this.log.info("adding custom header {} for post to {}", customHeader.getKey(),
								String.join(", ", platformsArray));
					}
					http.add(customHeader.getKey(), customHeader.getValue());
				}
			}) //
			.body(body) //
			.retrieve() //
			.body(JsonNode.class);
		Assert.notNull(json, "the json response should not be null");
		var status = nullOrNode(json, "status", JsonNode::asString);
		var id = nullOrNode(json, "id", JsonNode::asString);
		var refId = nullOrNode(json, "refId", JsonNode::asString);
		var validate = nullOrNode(json, "validate", JsonNode::asBoolean);
		var scheduleDate = nullOrNode(json, "scheduleDate",
				j -> Instant.from(ISO_INSTANT.parse(j.get("scheduleDate").asString())));
		var postIdsResult = nullOrNode(json, "postIds", (Function<JsonNode, Map<Platform, Response.Post>>) jsonNode -> {
			var postIds = new HashMap<Platform, Response.Post>();
			jsonNode.iterator().forEachRemaining(n -> {
				var status1 = from(nullOrNode(n, "status", JsonNode::asString));
				var id1 = nullOrNode(n, "id", JsonNode::asString);
				var postUrl = nullOrNode(n, "postUrl", jn -> URI.create(jn.asString()));
				var platform = nullOrNode(n, "platform", jn -> Platform.of(jn.asString()));
				var type = nullOrNode(n, "type", JsonNode::asString);
				var cid = nullOrNode(n, "cid", JsonNode::asString);
				var value = new Response.Post(type, status1, id1, cid, postUrl, platform);
				postIds.put(platform, value);
			});
			return postIds;
		});
		return new Response(from(status), postIdsResult, scheduleDate, id, refId, post, Boolean.TRUE.equals(validate));
	}

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(@NonNull RuntimeHints hints, ClassLoader classLoader) {
			var mcs = MemberCategory.values();
			for (var c : new Class<?>[] { JsonNode.class, Response.class, PostContext.class, Response.Post.class,
					Response.Status.class, Platform.class })
				hints.reflection().registerType(c, mcs);

		}

	}

}
