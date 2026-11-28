package com.joshlong.mogul.api.analysis;

import com.joshlong.mogul.api.Analysis;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.utils.DateUtils;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.time.OffsetDateTime;
import java.util.List;

@Controller
@RegisterReflectionForBinding({ ClientAnalysis.class, Analysis.Highlight.class })
class AnalysisController {

	private final MogulService mogulService;

	private final AnalysisService analysisService;

	AnalysisController(MogulService mogulService, AnalysisService analysisService) {
		this.mogulService = mogulService;
		this.analysisService = analysisService;
	}

	/** the analysis of the thing of this type and id, or null if it's never had one */
	@QueryMapping
	ClientAnalysis analysis(@Argument String type, @Argument Long id) {
		var mogul = this.mogulService.getCurrentMogul();
		var analyzable = this.analysisService.resolveAnalyzable(mogul.id(), id, type);
		var analysis = this.analysisService.analysis(mogul.id(), analyzable);
		return analysis == null ? null
				: new ClientAnalysis(type, id, DateUtils.forDate(analysis.created()), analysis.highlights());
	}

	/**
	 * asks for the thing to be analyzed. it comes back as an
	 * {@code analysis-completed-event} keyed by {@code type:id}.
	 * @return false if there's nothing to analyze yet
	 */
	@MutationMapping
	boolean analyze(@Argument String type, @Argument Long id) {
		var mogul = this.mogulService.getCurrentMogul();
		return this.analysisService.requestAnalysis(mogul.id(), id, type);
	}

}

record ClientAnalysis(String type, Long analyzableId, OffsetDateTime created, List<Analysis.Highlight> highlights) {
}
