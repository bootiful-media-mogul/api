package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.processors.ProcessorCompletedEvent;
import com.joshlong.mogul.api.processors.Processors;
import com.joshlong.mogul.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * summaries of the pages that notes point to.
 * <p>
 * it happens in two halves. the page is read in the processors module, which is the only
 * part of mogul that touches it: that's where the url is checked, the page fetched, and
 * the bytes made sure of before anything reads them, and what comes back is plain text,
 * never markup. then, here, the model summarizes that text with no tools at all (see
 * {@link NoteSummarizer}).
 * <p>
 * there are two ways in:
 * <ul>
 * <li>a clip: a note made with a url and no text, by a client that wants it summarized
 * (the clippers do; the studio doesn't). the summary is written into the note -- and a
 * title made from it, if the clip came without one -- but only if the note's text is
 * still what it was when the summary was asked for (see
 * {@link NoteService#appendSummary(Long, Long, String, String)}), so a replayed reply
 * can't write it twice, and it never lands on an edit it didn't see. the studio is told
 * with a {@code note-summarized-event}.</li>
 * <li>the studio's note editor: it asks for a summary of whatever url it has, saved or
 * not, and gets a request id. the summary comes back to it in a
 * {@code page-summarized-event} with that id, and goes into the text being edited; the
 * mogul saves it, or doesn't. nothing is written here.</li>
 * </ul>
 */
@Component
class NoteSummaries {

	/**
	 * the name of the {@code Processor} bean over in the processors module, which is also
	 * the name a reply for us comes back under.
	 */
	static final String PROCESSOR_ID = "webPageProcessor";

	// what the processor reads
	private static final String URL = "url";

	// what it writes back
	private static final String PAGE_TITLE = "pageTitle";

	private static final String PAGE_TEXT = "pageText";

	private static final String PAGE_REJECTION = "pageRejection";

	// what it doesn't look at, and hands back untouched
	private static final String MOGUL_ID = "mogulId";

	// a clip's: the note to write to, and its text as it was
	private static final String NOTE_ID = "noteId";

	private static final String NOTE_TEXT_HASH = "noteTextHash";

	// the editor's: what the studio will know its summary by
	private static final String SUMMARY_REQUEST_ID = "summaryRequestId";

	/**
	 * how a summary turned out, as the studio is told.
	 */
	enum Outcome {

		/** there's a summary */
		SUMMARIZED,

		/**
		 * the note's text changed while the summary was being written, so it isn't in it
		 */
		CHANGED,

		/** the processor wouldn't read the page; the reason is its rejection code */
		REJECTED,

		/** something went wrong reading the page or summarizing it */
		FAILED

	}

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final NoteService noteService;

	private final Processors processors;

	private final NoteSummarizer summarizer;

	NoteSummaries(NoteService noteService, Processors processors, NoteSummarizer summarizer) {
		this.noteService = noteService;
		this.processors = processors;
		this.summarizer = summarizer;
	}

	/**
	 * a note has just been made: if it's a clip -- a url and no text -- ask for the page
	 * to be summarized into it. a failure to ask costs the note its summary, never the
	 * note itself.
	 */
	void created(Note note) {
		if (note.url() == null || StringUtils.hasText(note.note()))
			return;
		var context = new HashMap<String, Object>();
		context.put(NOTE_ID, note.id());
		context.put(NOTE_TEXT_HASH, NoteService.textHash(note.note()));
		try {
			this.request(note.mogulId(), note.url(), context);
		} //
		catch (Exception e) {
			this.log.warn("could not ask for a summary of note #{}", note.id(), e);
		}
	}

	/**
	 * asks for a summary of the page at the url, for the note editor.
	 * @return what the {@code page-summarized-event} carrying the summary will be keyed
	 * by
	 */
	String summarizePage(Long mogulId, URI url) {
		var requestId = UUID.randomUUID().toString();
		try {
			this.request(mogulId, url, Map.of(SUMMARY_REQUEST_ID, requestId));
		} //
		catch (Exception e) {
			throw new RuntimeException("could not ask for a summary of [" + url + "]", e);
		}
		return requestId;
	}

	private void request(Long mogulId, URI url, Map<String, Object> returnAddress) throws Exception {
		var context = new HashMap<String, Object>(returnAddress);
		context.put(URL, url.toString());
		context.put(MOGUL_ID, mogulId);
		this.processors.process(PROCESSOR_ID, context);
	}

	/**
	 * not in a transaction: the model can take a while, and nothing here needs a
	 * connection held open while it thinks. the write at the end, if there is one, is its
	 * own transaction.
	 */
	@ApplicationModuleListener(propagation = Propagation.NOT_SUPPORTED)
	void onProcessorCompleted(ProcessorCompletedEvent event) {
		if (!PROCESSOR_ID.equals(event.processorId()))
			return;
		var context = event.context();
		var reply = context.get(SUMMARY_REQUEST_ID) instanceof String requestId
				? new EditorReply((Long) context.get(MOGUL_ID), requestId) : new NoteReply((Long) context.get(MOGUL_ID),
						(Long) context.get(NOTE_ID), (String) context.get(NOTE_TEXT_HASH));
		if (!event.success()) {
			this.log.warn("couldn't read the page for {}: {}", reply, event.error());
			reply.notify(Outcome.FAILED, "unreachable", null);
			return;
		}
		if (context.get(PAGE_REJECTION) instanceof String rejection) {
			this.log.info("the page for {} wasn't read: {}", reply, rejection);
			reply.notify(Outcome.REJECTED, rejection, null);
			return;
		}
		if (reply instanceof NoteReply noteReply && !this.stillWanted(noteReply))
			return;
		String summary;
		try {
			summary = this.summarizer.summarize((String) context.get(PAGE_TITLE), URI.create((String) context.get(URL)),
					(String) context.get(PAGE_TEXT));
		} //
		catch (RuntimeException e) {
			this.log.warn("couldn't summarize the page for {}", reply, e);
			reply.notify(Outcome.FAILED, "model", null);
			return;
		}
		if (reply instanceof NoteReply noteReply) {
			var updated = this.noteService.appendSummary(noteReply.mogulId(), noteReply.noteId(), noteReply.textHash(),
					summary);
			if (updated == null)
				reply.notify(Outcome.CHANGED, null, summary);
			else
				noteReply.summarized(summary, this.title(updated));
		}
		else
			reply.notify(Outcome.SUMMARIZED, null, summary);
	}

	/**
	 * a title for a clip that came without one -- a url shared from a phone, say -- made
	 * from its text, now that it has some. a clip with a title keeps it, and a title that
	 * can't be made costs the clip nothing but the title.
	 * @return the title it was given, or null
	 */
	private String title(Note note) {
		if (StringUtils.hasText(note.title()))
			return null;
		try {
			var title = this.summarizer.title(note.note());
			return this.noteService.fillTitle(note.mogulId(), note.id(), title) == null ? null : title;
		} //
		catch (RuntimeException e) {
			this.log.warn("couldn't make a title for note #{}", note.id(), e);
			return null;
		}
	}

	/**
	 * whether a clip's note is still there, and still has the text it had. if it's been
	 * edited, or this reply has already been handled, don't pay for a summary that has
	 * nowhere to go.
	 */
	private boolean stillWanted(NoteReply reply) {
		var note = this.noteService.getNoteById(reply.noteId());
		if (note == null || !note.mogulId().equals(reply.mogulId())) {
			this.log.info("note #{} is gone; dropping its summary", reply.noteId());
			return false;
		}
		if (!NoteService.textHash(note.note()).equals(reply.textHash())) {
			reply.notify(Outcome.CHANGED, null, null);
			return false;
		}
		return true;
	}

	/** who a summary goes back to, and how */
	private sealed interface Reply permits NoteReply, EditorReply {

		void notify(Outcome outcome, String reason, String summary);

		static void send(Long mogulId, Object event, String key, Map<String, Object> context, Outcome outcome,
				String reason, String summary) {
			var all = new HashMap<>(context);
			all.put("outcome", outcome.name().toLowerCase(Locale.ROOT));
			if (reason != null)
				all.put("reason", reason);
			if (summary != null)
				all.put("summary", summary);
			NotificationEvents
				.notifyAsync(NotificationEvent.systemNotificationEventFor(mogulId, event, key, JsonUtils.write(all)));
		}

	}

	/** a clip: the summary goes into the note */
	private record NoteReply(Long mogulId, Long noteId, String textHash) implements Reply {

		@Override
		public void notify(Outcome outcome, String reason, String summary) {
			this.send(outcome, reason, summary, Map.of("noteId", this.noteId));
		}

		/** the summary is in the note, and so is the title, if it was given one */
		void summarized(String summary, String title) {
			var context = new HashMap<String, Object>();
			context.put("noteId", this.noteId);
			if (title != null)
				context.put("title", title);
			this.send(Outcome.SUMMARIZED, null, summary, context);
		}

		private void send(Outcome outcome, String reason, String summary, Map<String, Object> context) {
			Reply.send(this.mogulId, new NoteSummarizedEvent(this.mogulId, this.noteId, outcome),
					Long.toString(this.noteId), context, outcome, reason, summary);
		}

		@Override
		public String toString() {
			return "note #" + this.noteId;
		}

	}

	/** the note editor: the summary goes back to it, and nowhere else */
	private record EditorReply(Long mogulId, String requestId) implements Reply {

		@Override
		public void notify(Outcome outcome, String reason, String summary) {
			Reply.send(this.mogulId, new PageSummarizedEvent(this.mogulId, this.requestId, outcome), this.requestId,
					Map.of("requestId", this.requestId), outcome, reason, summary);
		}

		@Override
		public String toString() {
			return "summary request " + this.requestId;
		}

	}

	/**
	 * what the studio is told about a clip, under the category
	 * {@code note-summarized-event}
	 */
	record NoteSummarizedEvent(Long mogulId, Long noteId, Outcome outcome) {
	}

	/**
	 * what the note editor is told, under the category {@code page-summarized-event}
	 */
	record PageSummarizedEvent(Long mogulId, String requestId, Outcome outcome) {
	}

}
