package com.joshlong.mogul.api.feeds;

public interface EntryMapper<T> {

	Entry map(T t) throws Exception;

}