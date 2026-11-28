package com.joshlong.mogul.api.analysis;

import com.joshlong.mogul.api.AbstractDomainService;
import com.joshlong.mogul.api.Analysis;
import com.joshlong.mogul.api.Analyzable;
import com.joshlong.mogul.api.AnalyzableResolver;
import com.joshlong.mogul.api.AnalyzableSection;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.JsonUtils;
import com.joshlong.mogul.utils.ReflectionUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

@Service
@Transactional
@SuppressWarnings("unchecked")
class DefaultAnalysisService extends AbstractDomainService<Analyzable, AnalyzableResolver<?>>
		implements AnalysisService {

	private static final JsonMapper JSON = JsonMapper.builder().build();

	private final RowMapper<Analysis> analysisRowMapper = (rs, _) -> new Analysis(rs.getLong("mogul_id"),
			rs.getLong("id"), new Date(rs.getTimestamp("created").getTime()), rs.getString("payload"),
			ReflectionUtils.classForName(rs.getString("payload_class")), readHighlights(rs.getString("highlights")));

	private final JdbcClient db;

	private final MogulService mogulService;

	private final ApplicationEventPublisher publisher;

	DefaultAnalysisService(Collection<AnalyzableResolver<?>> resolvers, JdbcClient db, MogulService mogulService,
			ApplicationEventPublisher publisher) {
		super(resolvers);
		this.db = db;
		this.mogulService = mogulService;
		this.publisher = publisher;
	}

	@Override
	public <T extends Analyzable> T resolveAnalyzable(Long mogulId, Long id, String type) {
		this.mogulService.assertAuthorizedMogul(mogulId);
		return this.owned(mogulId, id, this.classForType(type));
	}

	/** the analyzable, which must be the mogul's */
	private <T extends Analyzable> T owned(Long mogulId, Long id, Class<T> clazz) {
		var analyzable = this.findEntity(clazz, id);
		var owner = analyzable == null ? null : ((AnalyzableResolver<T>) this.findResolver(clazz)).mogulId(analyzable);
		Assert.state(owner != null && owner.equals(mogulId),
				"there is no " + clazz.getSimpleName() + " [" + id + "] for mogul [" + mogulId + "]");
		return analyzable;
	}

	@Override
	@Transactional(readOnly = true)
	public Analysis analysis(Long mogulId, Analyzable analyzable) {
		return this.db.sql("select * from analysis where mogul_id = ? and payload = ? and payload_class = ?")
			.params(mogulId, JsonUtils.write(analyzable.analyzableKey()), analyzable.getClass().getName())
			.query(this.analysisRowMapper)
			.optional()
			.orElse(null);
	}

	@Override
	public boolean requestAnalysis(Long mogulId, Long id, String type) {
		this.resolveAnalyzable(mogulId, id, type);
		if (this.sections(mogulId, id, type).isEmpty())
			return false;
		this.publisher.publishEvent(new AnalysisRequestedEvent(mogulId, type, id));
		return true;
	}

	@Override
	@Transactional(readOnly = true)
	public List<AnalyzableSection> sections(Long mogulId, Long id, String type) {
		Class<Analyzable> clazz = this.classForType(type);
		var analyzable = this.owned(mogulId, id, clazz);
		return ((AnalyzableResolver<Analyzable>) this.findResolver(clazz)).sections(analyzable);
	}

	@Override
	public Analysis write(Long mogulId, Long id, String type, List<Analysis.Highlight> highlights) {
		Class<Analyzable> clazz = this.classForType(type);
		var analyzable = this.owned(mogulId, id, clazz);
		this.db.sql("""
				insert into analysis (mogul_id, created, payload, payload_class, highlights)
				values (?, ?, ?, ?, ?)
				on conflict (mogul_id, payload, payload_class)
				do update set created = excluded.created, highlights = excluded.highlights
				""")
			.params(mogulId, new Date(), JsonUtils.write(analyzable.analyzableKey()), clazz.getName(),
					writeHighlights(highlights))
			.update();
		return this.analysis(mogulId, analyzable);
	}

	/** json written and read as a tree, so the native image needs no hints for it */
	static String writeHighlights(List<Analysis.Highlight> highlights) {
		var array = JSON.createArrayNode();
		for (var highlight : highlights)
			array.addObject()
				.put("kind", highlight.kind())
				.put("quote", highlight.quote())
				.put("reason", highlight.reason())
				.put("score", highlight.score())
				.put("section", highlight.section())
				.put("sectionName", highlight.sectionName());
		return JSON.writeValueAsString(array);
	}

	static List<Analysis.Highlight> readHighlights(String json) {
		var highlights = new ArrayList<Analysis.Highlight>();
		for (var node : JSON.readTree(json))
			highlights.add(new Analysis.Highlight(node.path("kind").asString(""), node.path("quote").asString(""),
					node.path("reason").asString(""), node.path("score").asInt(0), stringOrNull(node.path("section")),
					stringOrNull(node.path("sectionName"))));
		return highlights;
	}

	// analyses written before highlights had sections have none
	private static String stringOrNull(tools.jackson.databind.JsonNode node) {
		return node.isString() ? node.asString() : null;
	}

}
