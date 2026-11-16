package com.joshlong.mogul.api.search;

import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.SearchableResult;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.DateUtils;
import com.joshlong.mogul.utils.JsonUtils;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.stereotype.Controller;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Map;

@Controller
class SearchController {

	private final SearchService searchService;

	private final MogulService mogulService;

	SearchController(SearchService searchService, MogulService mogulService) {
		this.searchService = searchService;
		this.mogulService = mogulService;
	}

	@SchemaMapping
	OffsetDateTime created(SearchableResult<?> rankedSearchResult) {
		return DateUtils.forDate(rankedSearchResult.created());
	}

	@SchemaMapping
	String context(SearchableResult<?> result) {
		return JsonUtils.write(result.context());
	}

	@QueryMapping
	Collection<SearchableResult<Searchable>> search(@Argument String query, @Argument Map<String, Object> metadata) {
		return this.searchService.search(this.mogulService.getCurrentMogul().id(), query, metadata);
	}

}
