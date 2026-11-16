package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SchemaMapping;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.util.Collection;

/**
 * handles looking up details associated with all compositions.
 */
@Controller
class CompositionController {

	private final CompositionService compositionService;

	private final MogulService mogulService;

	private final Collection<ComposableOwnerResolver> ownerResolvers;

	CompositionController(CompositionService compositionService, MogulService mogulService,
			Collection<ComposableOwnerResolver> ownerResolvers) {
		this.compositionService = compositionService;
		this.mogulService = mogulService;
		this.ownerResolvers = ownerResolvers;
	}

	// ids are sequential, so they're easy to guess: check that the composition belongs to
	// something of the signed-in mogul's
	private Composition owned(Composition composition, String what) {
		var mogulId = this.mogulService.getCurrentMogul().id();
		var owner = composition == null ? null
				: this.ownerResolvers.stream()
					.filter(r -> r.supports(composition.payloadClass()))
					.findFirst()
					.map(r -> r.mogulId(composition.payloadClass(), Long.parseLong(composition.payload())))
					.orElse(null);
		Assert.state(owner != null && owner.equals(mogulId), () -> "there is no " + what + " for mogul #" + mogulId);
		return composition;
	}

	@SchemaMapping
	String markdown(Attachment attachment) {
		return this.compositionService.createMarkdownPreviewForAttachment(attachment);
	}

	@QueryMapping
	Composition compositionById(@Argument Long compositionId) {
		return this.owned(this.compositionService.getCompositionById(compositionId), "composition #" + compositionId);
	}

	@MutationMapping
	boolean deleteCompositionAttachment(@Argument Long compositionAttachmentId) {
		this.owned(this.compositionService.getCompositionByAttachmentId(compositionAttachmentId),
				"composition attachment #" + compositionAttachmentId);
		this.compositionService.deleteCompositionAttachmentyId(compositionAttachmentId);
		return true;
	}

	@MutationMapping
	Attachment createCompositionAttachment(@Argument Long compositionId) {
		this.owned(this.compositionService.getCompositionById(compositionId), "composition #" + compositionId);
		var mogulId = this.mogulService.getCurrentMogul().id();
		return this.compositionService.createCompositionAttachment(mogulId, compositionId, "");
	}

}
