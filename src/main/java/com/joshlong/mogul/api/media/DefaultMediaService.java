package com.joshlong.mogul.api.media;

import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.processors.ProcessorCompletedEvent;
import com.joshlong.mogul.api.processors.Processors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.MediaType;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

class DefaultMediaService implements MediaService {

	/**
	 * the names of the {@code Processor} beans over in the processors module, which are
	 * also the names a reply for us comes back under.
	 */
	static final String NORMALIZATION_PROCESSOR_ID = "mediaNormalizationProcessor";

	static final String PRODUCTION_PROCESSOR_ID = "audioProductionProcessor";

	static final String WAVEFORM_PROCESSOR_ID = "audioWaveformProcessor";

	// what neither of them looks at, and both hand back untouched
	static final String INPUT_MANAGED_FILE_ID = "inputManagedFileId";

	static final String OUTPUT_MANAGED_FILE_ID = "outputManagedFileId";

	// what the processors read
	private static final String INPUT_BUCKET = "inputBucket";

	private static final String INPUT_KEY = "inputKey";

	private static final String INPUT_FILENAME = "inputFilename";

	private static final String INPUT_CONTENT_TYPE = "inputContentType";

	private static final String INPUT_BUCKETS = "inputBuckets";

	private static final String INPUT_KEYS = "inputKeys";

	private static final String OUTPUT_BUCKET = "outputBucket";

	private static final String OUTPUT_KEY = "outputKey";

	private static final String KEPT_RANGES = "keptRanges";

	// what they write back
	private static final String OUTPUT_CONTENT_TYPE = "outputContentType";

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Processors processors;

	private final ManagedFileService managedFileService;

	private final ApplicationEventPublisher publisher;

	private final TransactionTemplate transactionTemplate;

	private final Map<String, Consumer<ProcessorCompletedEvent>> processorHandlers = new ConcurrentHashMap<>(
			Map.of(NORMALIZATION_PROCESSOR_ID, this::normalized, PRODUCTION_PROCESSOR_ID, this::produced,
					WAVEFORM_PROCESSOR_ID, this::waveformComputed));

	DefaultMediaService(Processors processors, ManagedFileService managedFileService,
			ApplicationEventPublisher publisher, TransactionTemplate transactionTemplate) {
		this.processors = processors;
		this.managedFileService = managedFileService;
		this.publisher = publisher;
		this.transactionTemplate = transactionTemplate;
	}

	@Override
	public void normalize(ManagedFile input, ManagedFile output, Map<String, Object> context) {
		this.normalize(input, output, "", context);
	}

	@Override
	public void normalize(ManagedFile input, ManagedFile output, String keptRanges, Map<String, Object> context) {
		if (!input.written()) {
			this.log.debug("the input file {} has not been written yet, so we can't normalize it", input.id());
			return;
		}
		var request = new HashMap<>(context);
		request.put(INPUT_BUCKET, input.bucket());
		request.put(INPUT_KEY, input.key());
		request.put(INPUT_FILENAME, input.filename());
		request.put(INPUT_CONTENT_TYPE, input.contentType());
		request.put(OUTPUT_BUCKET, output.bucket());
		request.put(OUTPUT_KEY, output.key());
		request.put(INPUT_MANAGED_FILE_ID, input.id());
		request.put(OUTPUT_MANAGED_FILE_ID, output.id());
		if (StringUtils.hasText(keptRanges))
			request.put(KEPT_RANGES, keptRanges);
		this.launch(NORMALIZATION_PROCESSOR_ID, request,
				() -> "could not request the normalization of ManagedFile #" + input.id());
		this.log.debug("requested the normalization of {} into {}", input.id(), output.id());
	}

	@Override
	public void produce(List<ManagedFile> inputs, ManagedFile output, Map<String, Object> context) {
		Assert.state(!inputs.isEmpty(), "there must be something to produce");
		for (var input : inputs)
			Assert.state(input.written(), () -> "ManagedFile #" + input.id() + " has nothing written to it yet");
		var buckets = new ArrayList<String>();
		var keys = new ArrayList<String>();
		for (var input : inputs) {
			buckets.add(input.bucket());
			keys.add(input.key());
		}
		var request = new HashMap<>(context);
		request.put(INPUT_BUCKETS, buckets);
		request.put(INPUT_KEYS, keys);
		request.put(OUTPUT_BUCKET, output.bucket());
		request.put(OUTPUT_KEY, output.key());
		request.put(OUTPUT_MANAGED_FILE_ID, output.id());
		this.launch(PRODUCTION_PROCESSOR_ID, request,
				() -> "could not request the production of ManagedFile #" + output.id());
		this.log.debug("requested the production of {} into {}", keys, output.id());
	}

	@Override
	public void waveform(ManagedFile input, ManagedFile output, Map<String, Object> context) {
		if (!input.written()) {
			this.log.debug("the input file {} has not been written yet, so there's no waveform to draw", input.id());
			return;
		}
		var request = new HashMap<>(context);
		request.put(INPUT_BUCKET, input.bucket());
		request.put(INPUT_KEY, input.key());
		request.put(INPUT_FILENAME, input.filename());
		request.put(OUTPUT_BUCKET, output.bucket());
		request.put(OUTPUT_KEY, output.key());
		request.put(INPUT_MANAGED_FILE_ID, input.id());
		request.put(OUTPUT_MANAGED_FILE_ID, output.id());
		this.launch(WAVEFORM_PROCESSOR_ID, request,
				() -> "could not request the waveform of ManagedFile #" + input.id());
		this.log.debug("requested the waveform of {} into {}", input.id(), output.id());
	}

	private void launch(String processorId, Map<String, Object> request, Supplier<String> failure) {
		try {
			this.processors.process(processorId, request);
		} //
		catch (Exception e) {
			throw new RuntimeException(failure.get(), e);
		}
	}

	@ApplicationModuleListener
	void onProcessorCompleted(ProcessorCompletedEvent event) {
		Assert.notNull(event, "the ProcessorCompletedEvent must not be null");
		if (this.processorHandlers.containsKey(event.processorId()))
			this.processorHandlers.get(event.processorId()).accept(event);
		else {
			this.log.warn("no handler for ProcessorCompletedEvent with processorId {}", event.processorId());
		}
	}

	private void normalized(ProcessorCompletedEvent event) {
		var context = event.context();
		var inputId = (Long) context.get(INPUT_MANAGED_FILE_ID);
		var outputId = (Long) context.get(OUTPUT_MANAGED_FILE_ID);
		if (!event.success()) {
			this.log.warn("the normalization of ManagedFile #{} into #{} failed: {}", inputId, outputId,
					this.reason(event));
			this.publish(new MediaNormalizationFailedEvent(this.managedFileService.getManagedFileById(inputId),
					this.managedFileService.getManagedFileById(outputId), this.reason(event), context));
			return;
		}
		var in = this.managedFileService.getManagedFileById(inputId);
		var output = this.managedFileService.getManagedFileById(outputId);
		this.managedFileService.refreshManagedFileFromStorage(output.id(), output.filename(),
				this.outputContentType(context, in));
		var out = this.managedFileService.getManagedFileById(output.id());
		this.publish(new MediaNormalizedEvent(in, out, context));
		this.log.debug("media normalization completed for {} into {}", in.id(), out.id());
	}

	private void produced(ProcessorCompletedEvent event) {
		var context = event.context();
		var outputId = (Long) context.get(OUTPUT_MANAGED_FILE_ID);
		if (!event.success()) {
			// this one is announced rather than merely logged. somebody asked for this
			// render and is waiting on it -- a publication, most likely -- and silence
			// leaves them waiting forever.
			this.log.warn("the production of ManagedFile #{} failed: {}", outputId, this.reason(event));
			this.publish(new AudioProducedEvent(this.managedFileService.getManagedFileById(outputId), false,
					event.error(), context));
			return;
		}
		var output = this.managedFileService.getManagedFileById(outputId);
		this.managedFileService.refreshManagedFileFromStorage(output.id(), output.filename(), CommonMediaTypes.MP3);
		var out = this.managedFileService.getManagedFileById(output.id());
		this.publish(new AudioProducedEvent(out, true, null, context));
		this.log.debug("audio production completed into {}", out.id());
	}

	private void waveformComputed(ProcessorCompletedEvent event) {
		var context = event.context();
		var inputId = (Long) context.get(INPUT_MANAGED_FILE_ID);
		var outputId = (Long) context.get(OUTPUT_MANAGED_FILE_ID);
		if (!event.success()) {
			// announced, not just logged: the segment editor is showing "drawing the
			// waveform" until it hears one way or the other.
			this.log.warn("drawing the waveform of ManagedFile #{} into #{} failed: {}", inputId, outputId,
					this.reason(event));
			this.publish(new AudioWaveformComputedEvent(this.managedFileService.getManagedFileById(inputId),
					this.managedFileService.getManagedFileById(outputId), false, this.reason(event), context));
			return;
		}
		var output = this.managedFileService.getManagedFileById(outputId);
		this.managedFileService.refreshManagedFileFromStorage(output.id(), "waveform.bin", CommonMediaTypes.BINARY);
		var in = this.managedFileService.getManagedFileById(inputId);
		var out = this.managedFileService.getManagedFileById(outputId);
		this.publish(new AudioWaveformComputedEvent(in, out, true, null, context));
		this.log.debug("the waveform of {} has been drawn into {}", in.id(), out.id());
	}

	private String reason(ProcessorCompletedEvent event) {
		return StringUtils.hasText(event.error()) ? event.error() : "no reason given";
	}

	private void publish(Object event) {
		this.transactionTemplate.executeWithoutResult(_ -> this.publisher.publishEvent(event));
	}

	private MediaType outputContentType(Map<String, Object> context, ManagedFile input) {
		if (context.get(OUTPUT_CONTENT_TYPE) instanceof String reported && StringUtils.hasText(reported))
			return MediaType.parseMediaType(reported);
		var inputContentType = MediaType.parseMediaType(input.contentType());
		return CommonMediaTypes.IMAGE.isCompatibleWith(inputContentType) ? CommonMediaTypes.JPG : CommonMediaTypes.MP3;
	}

}
