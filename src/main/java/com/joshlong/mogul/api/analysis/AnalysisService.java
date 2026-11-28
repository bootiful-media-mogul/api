package com.joshlong.mogul.api.analysis;

import com.joshlong.mogul.api.Analysis;
import com.joshlong.mogul.api.Analyzable;
import com.joshlong.mogul.api.AnalyzableSection;

import java.util.List;

public interface AnalysisService {

	/**
	 * the analyzable of this type ({@code episode}, {@code post}) and id, which must
	 * belong to the given mogul, who must be the one signed in.
	 */
	<T extends Analyzable> T resolveAnalyzable(Long mogulId, Long id, String type);

	/** the analyzable's analysis, or null if it's never been analyzed */
	Analysis analysis(Long mogulId, Analyzable analyzable);

	/**
	 * asks for the analyzable to be analyzed, which happens in the background; the studio
	 * hears of it in an {@code analysis-completed-event}. it replaces any analysis there
	 * was.
	 * @return false if there's no text to analyze yet (an episode with no transcripts),
	 * so nothing was asked for
	 */
	boolean requestAnalysis(Long mogulId, Long id, String type);

	/**
	 * the text of the analyzable, in sections (an episode's segments), if it belongs to
	 * the mogul. empty if there's no text yet. it doesn't ask who is signed in, so it can
	 * be used in the background.
	 */
	List<AnalyzableSection> sections(Long mogulId, Long id, String type);

	/** writes the analysis, in place of any there was */
	Analysis write(Long mogulId, Long id, String type, List<Analysis.Highlight> highlights);

}
