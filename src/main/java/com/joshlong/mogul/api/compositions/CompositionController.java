package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.io.Serializable;

@Controller
class CompositionController {

	private final CompositionService compositionService;

	private final MogulService mogulService;

	private final ManagedFileService managedFileService;

	CompositionController(CompositionService compositionService, MogulService mogulService,
			ManagedFileService managedFileService) {
		this.compositionService = compositionService;
		this.mogulService = mogulService;
		this.managedFileService = managedFileService;
	}
	/*
	 * @QueryMapping Composition compose(
	 *
	 * @Argument Class<?> type,
	 *
	 * @Argument Serializable publicationKey,
	 *
	 * @Argument String field) { var currentMogul = this.mogulService.getCurrentMogul();
	 * return this.compositionService.compose(currentMogul.id(), type, publicationKey,
	 * field); }
	 *
	 * @MutationMapping Attachment attachToComposition(
	 *
	 * @Argument Long compositionId,
	 *
	 * @Argument String key,
	 *
	 * @Argument Long managedFileId) { var managedFile =
	 * this.managedFileService.getManagedFile(managedFileId); return
	 * this.compositionService.attach(compositionId, key, managedFile); }
	 */

}
