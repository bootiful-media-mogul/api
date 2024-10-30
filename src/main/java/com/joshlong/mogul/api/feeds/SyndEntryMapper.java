package com.joshlong.mogul.api.feeds;

import com.rometools.rome.feed.synd.SyndEntry;

public interface SyndEntryMapper<T> {

	SyndEntry map(T t) throws Exception;

}
