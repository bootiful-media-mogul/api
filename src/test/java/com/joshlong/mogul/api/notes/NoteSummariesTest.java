package com.joshlong.mogul.api.notes;

import com.joshlong.mogul.api.Note;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.processors.ProcessorCompletedEvent;
import com.joshlong.mogul.utils.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * where a summary goes: into a clip's note, or back to the studio's editor and nowhere
 * else. the processor and the model are stand-ins; the notes are real, in the local
 * database (see {@code setup_local_dev_from_prod_db.sh}), and only the one made here is
 * removed.
 */
class NoteSummariesTest {

	private final JdbcClient db = JdbcClient.create(new DriverManagerDataSource(
			"jdbc:postgresql://" + System.getenv().getOrDefault("DB_HOST", "localhost") + "/mogul", "mogul", "mogul"));

	private final DefaultNoteService notes = new DefaultNoteService(List.of(), _ -> {
	}, this.db, null);

	private final List<Map<String, Object>> requests = new CopyOnWriteArrayList<>();

	private final List<NotificationEvent> notifications = new CopyOnWriteArrayList<>();

	private final AtomicInteger modelCalls = new AtomicInteger();

	private final ChatModel model = prompt -> {
		this.modelCalls.incrementAndGet();
		var titling = prompt.getSystemMessage().getText().contains("titles");
		var answer = titling ? "A Title for the Page" : "A summary of the page.";
		return new ChatResponse(List.of(new Generation(new AssistantMessage(answer))));
	};

	private final NoteSummaries summaries = new NoteSummaries(this.notes, (processorId, context) -> {
		assertThat(processorId).isEqualTo(NoteSummaries.PROCESSOR_ID);
		this.requests.add(context);
	}, new NoteSummarizer(this.model));

	private Long mogulId;

	private Long noteId;

	@BeforeAll
	static void json() throws Exception {
		// JsonUtils is handed its mapper by Spring, which isn't here
		var constructor = JsonUtils.class.getDeclaredConstructor(JsonMapper.class);
		constructor.setAccessible(true);
		constructor.newInstance(JsonMapper.builder().build());
	}

	@BeforeEach
	void setUp() {
		this.mogulId = this.db.sql("select min(id) from mogul").query(Long.class).single();
		// notifications go out through a static, on a thread of their own, in a
		// transaction; here the transaction is nothing and the notification is kept
		var beans = new StaticListableBeanFactory();
		beans.addBean("transactionTemplate", new TransactionTemplate(new NoTransactions()));
		var notificationEvents = new NotificationEvents();
		notificationEvents.setBeanFactory(beans);
		notificationEvents.setApplicationEventPublisher(event -> {
			if (event instanceof NotificationEvent notification)
				this.notifications.add(notification);
		});
	}

	@AfterEach
	void tearDown() {
		if (this.noteId != null)
			this.db.sql("delete from note where id = ?").param(this.noteId).update();
	}

	private Note note(String text) {
		return this.note(text, null);
	}

	private Note note(String text, String title) {
		var keys = new GeneratedKeyHolder();
		this.db.sql("""
				insert into note (mogul_id, payload, payload_class, url, note, title)
				values (?, ?, 'com.joshlong.mogul.api.mogul.Mogul', 'https://example.com/post', ?, ?)
				""").params(this.mogulId, this.mogulId.toString(), text, title).update(keys);
		this.noteId = ((Number) Objects.requireNonNull(keys.getKeys()).get("id")).longValue();
		return this.notes.getNoteById(this.noteId);
	}

	/** what the processor sends back: the request, plus whatever it made of the page */
	private void reply(Map<String, Object> request, Map<String, Object> result) {
		var context = new HashMap<>(request);
		context.putAll(result);
		this.summaries.onProcessorCompleted(new ProcessorCompletedEvent(NoteSummaries.PROCESSOR_ID, "a-correlation-id",
				context, Instant.now(), true, null));
	}

	private static Map<String, Object> page() {
		return Map.of("pageTitle", "A post", "pageText", "What the post says.", "pageTruncated", false);
	}

	private NotificationEvent onlyNotification() {
		await().atMost(Duration.ofSeconds(5)).until(() -> !this.notifications.isEmpty());
		assertThat(this.notifications).hasSize(1);
		return this.notifications.getFirst();
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> context(NotificationEvent notification) {
		return (Map<String, Object>) JsonUtils.read(notification.context(), Map.class);
	}

	@Test
	void aClipIsSummarizedIntoItsNote() {
		var note = this.note("");
		this.summaries.created(note);
		assertThat(this.requests).singleElement()
			.satisfies(request -> assertThat(request).containsEntry("url", "https://example.com/post")
				.containsEntry("noteId", note.id())
				.containsEntry("mogulId", this.mogulId)
				.containsEntry("noteTextHash", NoteService.textHash("")));

		this.reply(this.requests.getFirst(), page());

		var summarized = this.notes.getNoteById(note.id());
		assertThat(summarized.note()).isEqualTo("A summary of the page.");
		// it came without a title, so it's given one, made from what it says now
		assertThat(summarized.title()).isEqualTo("A Title for the Page");
		var notification = this.onlyNotification();
		assertThat(notification.category()).isEqualTo("note-summarized-event");
		assertThat(notification.key()).isEqualTo(note.id().toString());
		assertThat(context(notification)).containsEntry("outcome", "summarized")
			.containsEntry("summary", "A summary of the page.")
			.containsEntry("title", "A Title for the Page");
	}

	@Test
	void aClipKeepsTheTitleItCameWith() {
		var note = this.note("", "Podcast - Wikipedia");
		this.summaries.created(note);
		this.reply(this.requests.getFirst(), page());

		var summarized = this.notes.getNoteById(note.id());
		assertThat(summarized.note()).isEqualTo("A summary of the page.");
		assertThat(summarized.title()).isEqualTo("Podcast - Wikipedia");
		// the summary, and no title
		assertThat(this.modelCalls).hasValue(1);
		assertThat(context(this.onlyNotification())).doesNotContainKey("title");
	}

	@Test
	void aNoteWithTextIsNotAClip() {
		this.summaries.created(this.note("my own words"));
		assertThat(this.requests).isEmpty();
	}

	@Test
	void aClipEditedWhileItsPageWasReadKeepsItsEditAndCostsNothing() {
		var note = this.note("");
		this.summaries.created(note);
		this.db.sql("update note set note = 'typed meanwhile' where id = ?").param(note.id()).update();

		this.reply(this.requests.getFirst(), page());

		assertThat(this.notes.getNoteById(note.id()).note()).isEqualTo("typed meanwhile");
		assertThat(this.modelCalls).hasValue(0);
		assertThat(context(this.onlyNotification())).containsEntry("outcome", "changed");
	}

	@Test
	void theEditorGetsItsSummaryBackAndNothingIsWritten() {
		var note = this.note("");
		var requestId = this.summaries.summarizePage(this.mogulId, URI.create("https://example.com/post"));
		assertThat(this.requests).singleElement()
			.satisfies(request -> assertThat(request).containsEntry("summaryRequestId", requestId)
				.containsEntry("url", "https://example.com/post")
				.doesNotContainKey("noteId"));

		this.reply(this.requests.getFirst(), page());

		var notification = this.onlyNotification();
		assertThat(notification.category()).isEqualTo("page-summarized-event");
		assertThat(notification.key()).isEqualTo(requestId);
		assertThat(context(notification)).containsEntry("outcome", "summarized")
			.containsEntry("summary", "A summary of the page.");
		assertThat(this.notes.getNoteById(note.id()).note()).isEmpty();
	}

	@Test
	void theEditorIsToldWhyAPageWasNotRead() {
		var requestId = this.summaries.summarizePage(this.mogulId, URI.create("https://www.adobe.com/"));
		this.reply(this.requests.getFirst(), Map.of("pageRejection", "blocked"));

		var notification = this.onlyNotification();
		assertThat(notification.key()).isEqualTo(requestId);
		assertThat(context(notification)).containsEntry("outcome", "rejected")
			.containsEntry("reason", "blocked")
			.doesNotContainKey("summary");
		assertThat(this.modelCalls).hasValue(0);
	}

	@Test
	void repliesForOtherProcessorsAreNotOurs() {
		this.summaries.onProcessorCompleted(new ProcessorCompletedEvent("transcriptionProcessor", "x",
				Map.of("summaryRequestId", "x"), Instant.now(), true, null));
		assertThat(this.modelCalls).hasValue(0);
		assertThat(this.notifications).isEmpty();
	}

	/** transactions that do nothing, for the notifications */
	private static class NoTransactions extends AbstractPlatformTransactionManager {

		@Override
		protected Object doGetTransaction() {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) {
		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) {
		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) {
		}

	}

}
