package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.cleanvoice.Cleanvoice;
import com.joshlong.cleanvoice.model.Account;
import com.joshlong.cleanvoice.model.Edit;
import com.joshlong.cleanvoice.model.EditConfig;
import com.joshlong.cleanvoice.model.EditCreated;
import com.joshlong.cleanvoice.model.EditRequest;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.settings.SettingWrittenEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;

import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * the one {@link Cleanvoice} in the application, standing in for the one the
 * cleanvoice-client auto-configuration would otherwise register with a single,
 * application-wide key. every call is made with the key of whichever mogul it's on behalf
 * of: the one bound with {@link CleanvoiceTenant#callAs}, or else the signed-in one.
 * <p>
 * there's a real client per mogul and key. keying on the key as well as the mogul means a
 * changed key takes effect on every node at once: each looks the key up in settings,
 * whose caches are already evicted cluster-wide, and finds no client for the new one yet.
 * the settings-written listener only stops the old one from lingering.
 */
class MultitenantCleanvoice implements Cleanvoice {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private record Tenant(Long mogulId, String apiKey) {
	}

	private final Map<Tenant, Cleanvoice> clients = new ConcurrentHashMap<>();

	private final Function<String, Cleanvoice> clientForApiKey;

	private final CleanvoiceSettings settings;

	private final MogulService mogulService;

	MultitenantCleanvoice(Function<String, Cleanvoice> clientForApiKey, CleanvoiceSettings settings,
			MogulService mogulService) {
		this.clientForApiKey = clientForApiKey;
		this.settings = settings;
		this.mogulService = mogulService;
	}

	private Cleanvoice client() {
		var mogulId = CleanvoiceTenant.current().orElseGet(() -> this.mogulService.getCurrentMogul().id());
		var apiKey = this.settings.apiKey(mogulId);
		if (apiKey == null)
			throw new CleanvoiceNotConfiguredException(mogulId);
		return this.clients.computeIfAbsent(new Tenant(mogulId, apiKey), tenant -> {
			this.log.debug("creating a cleanvoice client for mogul #{}", tenant.mogulId());
			return this.clientForApiKey.apply(tenant.apiKey());
		});
	}

	@EventListener
	void settingWritten(SettingWrittenEvent event) {
		if (!CleanvoiceSettings.CATEGORY.equals(event.category()))
			return;
		this.clients.keySet().removeIf(tenant -> tenant.mogulId().equals(event.mogulId()));
	}

	@Override
	public Account account() {
		return this.client().account();
	}

	@Override
	public String upload(Path file) {
		return this.client().upload(file);
	}

	@Override
	public String upload(Resource resource) {
		return this.client().upload(resource);
	}

	@Override
	public String upload(Resource resource, String filename) {
		return this.client().upload(resource, filename);
	}

	@Override
	public EditCreated createEdit(EditRequest request) {
		return this.client().createEdit(request);
	}

	@Override
	public EditCreated createEdit(String fileUrl, EditConfig config) {
		return this.client().createEdit(fileUrl, config);
	}

	@Override
	public EditCreated createEdit(URI fileUri, EditConfig config) {
		return this.client().createEdit(fileUri, config);
	}

	@Override
	public EditCreated createEdit(URL fileUrl, EditConfig config) {
		return this.client().createEdit(fileUrl, config);
	}

	@Override
	public EditCreated createEdit(Path localFile, EditConfig config) {
		return this.client().createEdit(localFile, config);
	}

	@Override
	public EditCreated createEdit(Resource resource, EditConfig config) {
		return this.client().createEdit(resource, config);
	}

	@Override
	public EditCreated createMultitrackEdit(List<Path> localTracks, EditConfig config) {
		return this.client().createMultitrackEdit(localTracks, config);
	}

	@Override
	public EditCreated createMultitrackEdit(Collection<? extends Resource> tracks, EditConfig config) {
		return this.client().createMultitrackEdit(tracks, config);
	}

	@Override
	public Edit getEdit(String editId) {
		return this.client().getEdit(editId);
	}

	@Override
	public void deleteEdit(String editId) {
		this.client().deleteEdit(editId);
	}

	@Override
	public Edit awaitCompletion(String editId, Duration timeout) throws InterruptedException, TimeoutException {
		return this.client().awaitCompletion(editId, timeout);
	}

	@Override
	public Edit process(Path localFile, EditConfig config, Duration timeout)
			throws InterruptedException, TimeoutException {
		return this.client().process(localFile, config, timeout);
	}

	@Override
	public Edit process(URI fileUri, EditConfig config, Duration timeout)
			throws InterruptedException, TimeoutException {
		return this.client().process(fileUri, config, timeout);
	}

	@Override
	public Edit process(URL fileUrl, EditConfig config, Duration timeout)
			throws InterruptedException, TimeoutException {
		return this.client().process(fileUrl, config, timeout);
	}

	@Override
	public Edit process(Resource resource, EditConfig config, Duration timeout)
			throws InterruptedException, TimeoutException {
		return this.client().process(resource, config, timeout);
	}

}
