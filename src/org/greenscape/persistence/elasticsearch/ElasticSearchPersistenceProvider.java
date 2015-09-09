package org.greenscape.persistence.elasticsearch;

import org.greenscape.persistence.PersistenceProvider;
import org.greenscape.persistence.PersistenceType;

public final class ElasticSearchPersistenceProvider implements PersistenceProvider {
	static final String PROVIDER_NAME = "Elasticsearch";

	@Override
	public String getName() {
		return PROVIDER_NAME;
	}

	@Override
	public PersistenceType getType() {
		return PersistenceType.DOCUMENT;
	}

}
