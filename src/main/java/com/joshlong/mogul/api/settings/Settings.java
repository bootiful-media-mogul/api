package com.joshlong.mogul.api.settings;

import com.joshlong.mogul.utils.CollectionUtils;
import org.springframework.cache.Cache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class Settings {

	private final ApplicationEventPublisher publisher;

	private final JdbcClient db;

	private final TextEncryptor encryptor;

	private final SettingsRowMapper rowMapper;

	private final Cache mogulCache, mogulCategoryCache, mogulCategoryKeyCache;

	/**
	 * one lock per mogul. a cold page fires a dozen settings lookups at once; they all
	 * miss before any of them finishes, so the cache alone cannot help. the first through
	 * the gate reads, the rest wait and then find it already there.
	 * <p>
	 * evicting, so it cannot grow without bound. the worst an eviction can do is hand two
	 * threads separate locks for the same mogul and let them both read -- the same
	 * harmless duplicate this mechanism only ever set out to make rare, and vanishingly
	 * unlikely given a lock is held for the length of a single query.
	 */
	private final ConcurrentMap<Long, ReentrantLock> loadLocks;

	public Settings(ApplicationEventPublisher publisher, JdbcClient db, TextEncryptor encryptor, int maxCacheEntries,
			Cache mogulCache, Cache mogulCategoryCache, Cache mogulCategoryKeyCache) {
		this.loadLocks = CollectionUtils.evictingConcurrentMap(maxCacheEntries, Duration.ofMinutes(10));
		this.publisher = publisher;
		this.db = db;
		this.encryptor = encryptor;
		this.mogulCache = mogulCache;
		this.mogulCategoryCache = mogulCategoryCache;
		this.mogulCategoryKeyCache = mogulCategoryKeyCache;
		Assert.notNull(this.mogulCache, "the mogul cache is not null");
		Assert.notNull(this.mogulCategoryCache, "the mogul/category cache is not null");
		Assert.notNull(this.mogulCategoryKeyCache, "the mogul/category/key cache is not null");
		Assert.notNull(this.encryptor, "the encryptor must be non-null");
		Assert.notNull(this.db, "the db must be non-null");
		this.rowMapper = new SettingsRowMapper(encryptor);
	}

	public Map<String, String> getAllValuesByCategory(Long mogulId, String category) {
		var all = this.getAllSettingsByCategory(mogulId, category);
		var res = new HashMap<String, String>();
		for (var a : all.keySet())
			res.put(a, all.get(a).value());
		return res;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Setting> getAllSettingsByCategory(Long mogulId, String category) {
		var cacheKey = this.mogulCategoryCacheKey(mogulId, category);
		var hit = this.mogulCategoryCache.get(cacheKey);
		if (null != hit) {
			return (Map<String, Setting>) hit.get();
		}
		var forCategory = this.loadAllSettingsFor(mogulId).getOrDefault(category, Map.of());
		// cached even when empty. a plugin the mogul has never configured has no rows at
		// all, and without this every request for it would miss and re-read the lot.
		this.mogulCategoryCache.put(cacheKey, forCategory);
		return forCategory;
	}

	@SuppressWarnings("unchecked")
	private Setting get(Long mogulId, String category, String key) {
		var cacheKey = this.mogulCategoryKeyCacheKey(mogulId, category, key);
		var hit = this.mogulCategoryKeyCache.get(cacheKey);
		var settings = (List<Setting>) null;
		if (null != hit) {
			settings = (List<Setting>) hit.get();
		} //
		else {
			var setting = this.loadAllSettingsFor(mogulId).getOrDefault(category, Map.of()).get(key);
			settings = (null == setting) ? List.of() : List.of(setting);
			this.mogulCategoryKeyCache.put(cacheKey, settings);
		}
		Assert.state(Objects.requireNonNull(settings).size() <= 1,
				"there should never be more than one setting configured.");
		return settings.isEmpty() ? null : settings.getFirst();
	}

	/**
	 * one read for everything a mogul has, warming the other two caches on the way past.
	 * a mogul's settings are a handful of rows, so reading all of them costs less than
	 * reading any two of them separately.
	 * <p>
	 * the double-check inside the lock is not ceremonial: by the time a waiter acquires
	 * it the winner has usually already filled the cache, and the waiter has to see that
	 * rather than read again.
	 * <p>
	 * explicit get/put rather than the cache's own loader form throughout -- that form is
	 * backed by {@code computeIfAbsent}, and writing other entries into the same cache
	 * from inside it is a recursive update the underlying cache forbids.
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Map<String, Setting>> loadAllSettingsFor(Long mogulId) {
		var mogulCacheKey = Long.toString(mogulId);
		var hit = this.mogulCache.get(mogulCacheKey);
		if (null != hit) {
			return (Map<String, Map<String, Setting>>) hit.get();
		}
		var lock = this.loadLocks.computeIfAbsent(mogulId, _ -> new ReentrantLock());
		lock.lock();
		try {
			hit = this.mogulCache.get(mogulCacheKey);
			if (null != hit) {
				return (Map<String, Map<String, Setting>>) hit.get();
			}
			var settings = this.db //
				.sql("select * from settings where mogul_id = ?") //
				.param(mogulId) //
				.query(this.rowMapper) //
				.list();
			var byCategory = new HashMap<String, Map<String, Setting>>();
			for (var setting : settings)
				byCategory.computeIfAbsent(setting.category(), _ -> new HashMap<>()).put(setting.key(), setting);
			var immutable = new HashMap<String, Map<String, Setting>>();
			byCategory.forEach((category, values) -> immutable.put(category, Map.copyOf(values)));
			immutable.forEach((category, values) -> {
				this.mogulCategoryCache.put(this.mogulCategoryCacheKey(mogulId, category), values);
				values.forEach((key, setting) -> this.mogulCategoryKeyCache
					.put(this.mogulCategoryKeyCacheKey(mogulId, category, key), List.of(setting)));
			});
			var result = Map.copyOf(immutable);
			this.mogulCache.put(mogulCacheKey, result);
			return result;
		} //
		finally {
			lock.unlock();
		}
	}

	public String getValue(Long mogulId, String category, String key) {
		var v = this.get(mogulId, category, key);
		if (v != null)
			return v.value();
		return null;
	}

	@Transactional
	public void set(Long mogulId, String category, String key, String value) {
		this.db.sql("""
				insert into settings(mogul_id , category, key, value)
				values (? ,? ,? ,? )
				on conflict on constraint settings_mogul_id_category_key_key
				 		do update set value = excluded.value
				""") //
			.params(mogulId, category, key, this.encryptor.encrypt(value))//
			.update();
		// the key's own entry, the category map it lives inside, and the whole-mogul
		// entry -- which has to go too, or it would keep serving the old value straight
		// past the other two evictions. the next read re-reads everything in one query,
		// and writes are rare enough that that is the cheaper side of the trade.
		this.mogulCategoryCache.evictIfPresent(this.mogulCategoryCacheKey(mogulId, category));
		this.mogulCategoryKeyCache.evictIfPresent(this.mogulCategoryKeyCacheKey(mogulId, category, key));
		this.mogulCache.evictIfPresent(Long.toString(mogulId));
		this.publisher.publishEvent(new SettingWrittenEvent(mogulId, category, key, value));
	}

	private String mogulCategoryCacheKey(Long mogulId, String category) {
		return mogulId + "::" + category;
	}

	private String mogulCategoryKeyCacheKey(Long mogulId, String category, String key) {
		return mogulId + "::" + category + "::" + key;
	}

	public record Setting(String category, String key, String value) {
	}

}

class SettingsRowMapper implements RowMapper<Settings.Setting> {

	private final TextEncryptor encryptor;

	SettingsRowMapper(TextEncryptor encryptor) {
		this.encryptor = encryptor;
		Assert.notNull(this.encryptor, "the " + TextEncryptor.class.getName() + " must be non-null");
	}

	@Override
	public Settings.Setting mapRow(ResultSet rs, int rowNum) throws SQLException {
		return new Settings.Setting(rs.getString("category"), rs.getString("key"),
				encryptor.decrypt(rs.getString("value")));
	}

}
