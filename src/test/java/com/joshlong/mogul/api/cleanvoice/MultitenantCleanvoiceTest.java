package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.cleanvoice.Cleanvoice;
import com.joshlong.cleanvoice.model.Account;
import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.settings.SettingWrittenEvent;
import com.joshlong.mogul.api.settings.Settings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MultitenantCleanvoiceTest {

	private final Map<Long, String> keys = new HashMap<>();

	private final List<String> clientsCreatedFor = new ArrayList<>();

	private final Settings settings = mock(Settings.class);

	private final MogulService mogulService = mock(MogulService.class);

	private MultitenantCleanvoice cleanvoice;

	@BeforeEach
	void setUp() {
		when(this.settings.getValue(anyLong(), eq(CleanvoiceSettings.CATEGORY), eq(CleanvoiceSettings.API_KEY)))
			.thenAnswer(invocation -> this.keys.get((Long) invocation.getArgument(0)));
		this.cleanvoice = new MultitenantCleanvoice(apiKey -> {
			this.clientsCreatedFor.add(apiKey);
			var client = mock(Cleanvoice.class);
			// each client answers with its own key, so the test can see which one it got
			when(client.account()).thenReturn(new Account(null, Map.of("key", apiKey)));
			return client;
		}, new CleanvoiceSettings(this.settings), this.mogulService);
	}

	private void signedIn(long mogulId) {
		var mogul = mock(Mogul.class);
		when(mogul.id()).thenReturn(mogulId);
		when(this.mogulService.getCurrentMogul()).thenReturn(mogul);
	}

	private Object keyUsed() {
		return this.cleanvoice.account().meta().get("key");
	}

	@Test
	void eachMogulGetsTheirOwnKey() {
		this.keys.put(1L, "key-one");
		this.keys.put(2L, "key-two");
		this.signedIn(1);
		assertThat(this.keyUsed()).isEqualTo("key-one");
		this.signedIn(2);
		assertThat(this.keyUsed()).isEqualTo("key-two");
		this.signedIn(1);
		assertThat(this.keyUsed()).isEqualTo("key-one");
		assertThat(this.clientsCreatedFor).containsExactly("key-one", "key-two");
	}

	@Test
	void aChangedKeyGetsANewClient() {
		this.keys.put(1L, "old-key");
		this.signedIn(1);
		assertThat(this.keyUsed()).isEqualTo("old-key");
		this.keys.put(1L, "new-key");
		assertThat(this.keyUsed()).isEqualTo("new-key");
	}

	@Test
	void aBackgroundJobSaysWhoItsWorkingFor() throws Exception {
		this.keys.put(1L, "key-one");
		this.keys.put(2L, "key-two");
		this.signedIn(1);
		var used = CleanvoiceTenant.callAs(2L, this::keyUsed);
		assertThat(used).isEqualTo("key-two");
		// and only for as long as it says so
		assertThat(this.keyUsed()).isEqualTo("key-one");
	}

	@Test
	void noKeyIsAClearFailure() {
		this.signedIn(3);
		assertThatThrownBy(() -> this.cleanvoice.account()).isInstanceOf(CleanvoiceNotConfiguredException.class)
			.hasMessageContaining("#3");
	}

	@Test
	void writingTheSettingForgetsOnlyThatMogulsClients() {
		this.keys.put(1L, "key-one");
		this.keys.put(2L, "key-two");
		this.signedIn(1);
		this.keyUsed();
		this.signedIn(2);
		this.keyUsed();
		this.cleanvoice.settingWritten(new SettingWrittenEvent(1L, CleanvoiceSettings.CATEGORY, "apiKey", "x"));
		this.cleanvoice.settingWritten(new SettingWrittenEvent(2L, "podbean", "clientId", "x"));
		this.signedIn(1);
		this.keyUsed();
		this.signedIn(2);
		this.keyUsed();
		assertThat(this.clientsCreatedFor).containsExactly("key-one", "key-two", "key-one");
	}

}
