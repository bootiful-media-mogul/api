package com.joshlong.mogul.api;

public abstract class AbstractAnalyzableResolver<T extends Analyzable> extends AbstractDomainResolver<Analyzable, T>
		implements AnalyzableResolver<T> {

	protected AbstractAnalyzableResolver(Class<T> entityClass) {
		super(entityClass);
	}

}
