package com.joshlong.mogul.api.search.elastic;

import com.joshlong.mogul.api.AbstractDomainService;
import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.SearchableDeletedEvent;
import com.joshlong.mogul.api.SearchableResolver;
import com.joshlong.mogul.api.SearchableResult;
import com.joshlong.mogul.api.search.SearchService;
import com.joshlong.mogul.api.transcripts.TranscriptRecordedEvent;
import com.joshlong.mogul.utils.ReflectionUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.DeleteQuery;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.*;

@Lazy
interface DocumentRepository extends ElasticsearchRepository<Document, String> {

}

@Service
@SuppressWarnings("unchecked")
class ElasticSearchService extends AbstractDomainService<Searchable, SearchableResolver<?>> implements SearchService {

	static final String INDEX_NAME = "searchables";

	// how many searchables the rebuild loads and writes at a time
	private static final int BATCH_SIZE = 100;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final DocumentRepository documentRepository;

	private final ElasticsearchOperations ops;

	ElasticSearchService(Collection<SearchableResolver<?>> resolvers, @Lazy DocumentRepository documentRepository,
			@Lazy ElasticsearchOperations ops) {
		super(resolvers);
		this.documentRepository = documentRepository;
		this.ops = ops;
	}

	/**
	 * an entry's id names its type as well as its id: posts, segments, and notes are
	 * numbered separately, so post 5 and note 5 would otherwise share one entry, and
	 * whichever was indexed last would replace the other.
	 */
	private String entryId(Class<?> type, long searchableId) {
		return this.typeForClass(type) + ":" + searchableId;
	}

	@Override
	public void index(Searchable searchable) {
		this.log.info("indexing for searchable {}", searchable);
		var searchableClass = (Class<? extends Searchable>) searchable.getClass();
		var resolver = this.findResolver(searchableClass);
		Assert.notNull(resolver, () -> "there's no resolver for " + searchableClass.getName() + "!");
		var results = resolver.results(List.of(searchable.searchableId()));
		var document = results.isEmpty() ? null : this.documentFor(results.getFirst());
		if (document != null)
			this.documentRepository.save(document);
		else
			this.delete(searchableClass, searchable.searchableId());
	}

	@Override
	public void delete(Class<? extends Searchable> type, Long searchableId) {
		var id = this.entryId(type, searchableId);
		try {
			this.ops.delete(id, Document.class);
			this.log.debug("deleted the search entry {}", id);
		} //
		catch (RuntimeException e) {
			this.log.warn("couldn't delete the search entry {}", id, e);
		}
	}

	@ApplicationModuleListener
	void searchableDeleted(SearchableDeletedEvent event) {
		this.delete(event.type(), event.searchableId());
	}

	/**
	 * the entry for a result, or {@code null} if there's nothing worth indexing: no
	 * title, or no owner to file it under.
	 */
	private @Nullable Document documentFor(SearchableResult<?> result) {
		if (!StringUtils.hasText(result.title()) || result.mogulId() == null)
			return null;
		var type = result.searchable().getClass();
		return new Document(this.entryId(type, result.searchableId()), result.searchableId(), result.title(),
				Instant.now(), StringUtils.hasText(result.text()) ? result.text() : "", type.getName(),
				result.mogulId());
	}

	@Override
	public <T extends Searchable> Collection<SearchableResult<T>> search(Long mogulId, String shouldContain,
			Map<String, Object> metadata) {
		Assert.notNull(mogulId, "searches are always for a mogul");
		this.log.info("searching for [{}] for mogul {} with metadata {}", shouldContain, mogulId, metadata);
		var scope = metadata == null ? Map.<String, Object>of() : metadata;
		var type = scope.get("type") == null ? null : this.classForType(scope.get("type").toString());
		var nativeQuery = NativeQuery.builder() //
			.withQuery(q -> q //
				.bool(b -> {
					// only the mogul's own entries: a filter, so elasticsearch does it,
					// and
					// it doesn't change how the matches are ranked
					b.filter(f -> f.term(t -> t.field("mogulId").value(mogulId)));
					// only one kind of thing, if the metadata says so: also a filter, so
					// the hits aren't spent on other kinds
					if (type != null)
						b.filter(f -> f.matchPhrase(m -> m.field("className").query(type.getName())));
					b.must(m -> m.multiMatch(
							mm -> mm.query(shouldContain).fields("title^2", "description^2").fuzziness("AUTO")));
					// should match (boosts relevance if present)
					if (StringUtils.hasText(shouldContain)) {
						b.should(s -> s.match(mt -> mt.field("description").query(shouldContain)));
					}
					return b;
				})) //
			.withMaxResults(1000) //
			.build();
		var hits = this.ops.search(nativeQuery, Document.class);
		return this.resultsFor(mogulId, hits)
			.stream()
			.filter(r -> this.within(r, scope))
			.map(r -> (SearchableResult<T>) r)
			.toList();
	}

	/**
	 * whether a result is within what the search's metadata asks for: every entry of the
	 * metadata must be in the result's context, as with {@code {"type": "post", "blogId":
	 * 7}} for one blog's posts. numbers from json and from java compare by their text.
	 */
	private boolean within(SearchableResult<?> result, Map<String, Object> metadata) {
		for (var entry : metadata.entrySet())
			if (!String.valueOf(entry.getValue()).equals(String.valueOf(result.context().get(entry.getKey()))))
				return false;
		return true;
	}

	private record Entry(Class<? extends Searchable> type, long searchableId) {
	}

	private List<SearchableResult<? extends Searchable>> resultsFor(Long mogulId, SearchHits<Document> hits) {
		var ranks = new HashMap<Entry, Float>();
		var idsByType = new LinkedHashMap<Class<? extends Searchable>, List<Long>>();
		for (var hit : hits) {
			var document = hit.getContent();
			var type = (Class<? extends Searchable>) ReflectionUtils.classForName(document.className());
			ranks.put(new Entry(type, document.searchableId()), hit.getScore());
			idsByType.computeIfAbsent(type, _ -> new ArrayList<>()).add(document.searchableId());
		}
		// one result per thing: the segments of an episode, say, come back as the episode
		// once, at the rank of its best match. keyed by type as well as aggregate id,
		// since a post and an episode can have the same id
		var best = new HashMap<String, RankedSearchableResult<? extends Searchable>>();
		var dead = new ArrayList<Entry>();
		for (var entry : idsByType.entrySet()) {
			var type = entry.getKey();
			var ids = entry.getValue();
			var resolver = this.findResolver(type);
			Assert.notNull(resolver, () -> "there must be a valid resolver for " + type);
			var resolved = new HashSet<Long>();
			for (var result : resolver.results(ids)) {
				resolved.add(result.searchableId());
				// the filter already did this; it's here in case an entry's owner is
				// stale
				if (!mogulId.equals(result.mogulId()))
					continue;
				var rank = ranks.getOrDefault(new Entry(type, result.searchableId()), 0.0f);
				best.merge(result.type() + ":" + result.aggregateId(), new RankedSearchableResult<>(rank, result),
						(a, b) -> a.rank() >= b.rank() ? a : b);
			}
			for (var id : ids)
				if (!resolved.contains(id))
					dead.add(new Entry(type, id));
		}
		// entries for things that are gone: not results, and not worth keeping
		for (var entry : dead)
			this.delete(entry.type(), entry.searchableId());
		return best.values()
			.stream()
			.sorted((a, b) -> Float.compare(b.rank(), a.rank()))
			.<SearchableResult<? extends Searchable>>map(RankedSearchableResult::searchableResult)
			.toList();
	}

	@Override
	public int rebuild(Long mogulId) {
		Assert.notNull(mogulId, "a rebuild is always for a mogul");
		var indexOps = this.ops.indexOps(Document.class);
		// the index's mapping, declared rather than guessed from the first document, so
		// that mogulId is there to filter on from the start
		if (indexOps.exists())
			indexOps.putMapping();
		else
			indexOps.createWithMapping();
		// the mogul's entries, and any from before entries had owners: nobody can find
		// those any more, and the rebuild writes the mogul's back
		var stale = NativeQuery.builder()
			.withQuery(q -> q.bool(b -> b.should(s -> s.term(t -> t.field("mogulId").value(mogulId)))
				.should(s -> s.bool(n -> n.mustNot(m -> m.exists(e -> e.field("mogulId")))))
				.minimumShouldMatch("1")))
			.build();
		this.ops.delete(DeleteQuery.builder(stale).build(), Document.class);
		var written = 0;
		for (var resolver : this.resolvers) {
			var ids = new ArrayList<>(resolver.searchableIds(mogulId));
			for (var from = 0; from < ids.size(); from += BATCH_SIZE) {
				var batch = ids.subList(from, Math.min(ids.size(), from + BATCH_SIZE));
				var documents = new ArrayList<Document>();
				for (var result : resolver.results(batch)) {
					if (!mogulId.equals(result.mogulId()))
						continue;
					var document = this.documentFor(result);
					if (document != null)
						documents.add(document);
				}
				if (!documents.isEmpty())
					this.documentRepository.saveAll(documents);
				written += documents.size();
			}
		}
		indexOps.refresh();
		this.log.info("rebuilt the search index for mogul {}: {} entries", mogulId, written);
		return written;
	}

	@ApplicationModuleListener
	void indexForSearchOnTranscriptCompletion(TranscriptRecordedEvent event) {
		var aClazz = (Class<? extends Searchable>) event.type();
		var repo = this.findResolver(aClazz);
		var transcribable = Objects.requireNonNull(repo).find(event.transcribableId());
		Assert.notNull(transcribable, "the transcribable id " + event.transcribableId() + " was null!");
		this.index(transcribable);
	}

}

record RankedSearchableResult<T extends Searchable>(float rank, SearchableResult<T> searchableResult) {

}

/**
 * an entry in the index. {@code id} is the type and the id ({@code note:5});
 * {@code mogulId} is the owner, which every search filters on.
 */
@org.springframework.data.elasticsearch.annotations.Document(indexName = ElasticSearchService.INDEX_NAME)
record Document(@Id String id, @Field(type = FieldType.Long) Long searchableId,
		@Field(type = FieldType.Text, analyzer = "english") String title,
		@Field(type = FieldType.Date, format = DateFormat.epoch_millis) Instant when,
		@Field(type = FieldType.Text, analyzer = "english") String description,
		@Field(type = FieldType.Text) String className, @Field(type = FieldType.Long) Long mogulId) {
}
