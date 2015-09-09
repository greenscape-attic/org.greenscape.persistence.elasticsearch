package org.greenscape.persistence.elasticsearch;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.greenscape.core.ModelResource;
import org.greenscape.core.Property;
import org.greenscape.core.ResourceRegistry;
import org.greenscape.core.model.DocumentModel;
import org.greenscape.core.model.PersistedModelBase;
import org.greenscape.elasticsearch.client.Connection;
import org.greenscape.persistence.IdGenerator;
import org.greenscape.persistence.PersistenceProvider;
import org.greenscape.persistence.PersistenceService;
import org.greenscape.persistence.PersistenceType;
import org.greenscape.persistence.Query;
import org.greenscape.persistence.TypedQuery;
import org.greenscape.persistence.criteria.CriteriaBuilder;
import org.greenscape.persistence.criteria.CriteriaDelete;
import org.greenscape.persistence.criteria.CriteriaQuery;
import org.greenscape.persistence.criteria.CriteriaUpdate;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.log.LogService;

@Component(property = { "dbName=" + ElasticSearchPersistenceProvider.PROVIDER_NAME })
public class ElasticSearchPersistence implements PersistenceService {
	private static final PersistenceProvider provider;

	private Connection connection;
	private ResourceRegistry resourceRegistry;
	private TypeMapper typeMapper;

	/** Infrastructure services **/
	private BundleContext context;
	private LogService logService;

	static {
		provider = new ElasticSearchPersistenceProvider();
	}

	@Override
	public PersistenceProvider getProvider() {
		return provider;
	}

	@Override
	public PersistenceType getType() {
		return provider.getType();
	}

	@Activate
	void activate(ComponentContext ctx, Map<String, Object> config) {
		context = ctx.getBundleContext();
		typeMapper = new TypeMapper();
	}

	@Reference(policy = ReferencePolicy.DYNAMIC)
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public void unsetConnection(Connection connection) {
		this.connection = null;
	}

	@Reference(policy = ReferencePolicy.DYNAMIC)
	public void setResourceRegistry(ResourceRegistry resourceRegistry) {
		this.resourceRegistry = resourceRegistry;
	}

	public void unsetResourceRegistry(ResourceRegistry resourceRegistry) {
		this.resourceRegistry = resourceRegistry;
	}

	@Reference(cardinality = ReferenceCardinality.OPTIONAL, policy = ReferencePolicy.DYNAMIC)
	public void setLogService(LogService logService) {
		this.logService = logService;
	}

	public void unsetLogService(LogService logService) {
		this.logService = null;
	}

	@Override
	public <T> void save(String modelName, T object) {
		persist(modelName, (DocumentModel) object);
	}

	@Override
	public <T> void save(T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void save(Collection<T> objects) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void save(T[] objects) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void update(String modelName, T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void update(T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void saveOrUpdate(T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void remove(T object) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void remove(Collection<T> objects) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> void remove(T[] objects) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends DocumentModel> void delete(String modelName) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends DocumentModel> void delete(Class<T> clazz) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends DocumentModel> void delete(String modelName, String modelId) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends DocumentModel> void delete(Class<T> clazz, String modelId) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T extends DocumentModel> void delete(T documentModel) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object executeQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Collection<T> executeQuery(Class<T> clazz, String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Collection<T> executeQuery(Class<T> clazz, String query, int maxResult) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DocumentModel> List<T> find(String modelName) {
		return findByProperties(modelName, null);
	}

	@Override
	public <T extends DocumentModel> List<T> find(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DocumentModel> T find(String modelName, Object id) {
		return findByModelId(modelName, id.toString());
	}

	@Override
	public <T extends DocumentModel> T findByModelId(String modelName, String modelId) {
		List<T> list = findByProperty(modelName, DocumentModel.MODEL_ID, modelId);
		if (list == null || list.isEmpty())
			return null;
		else
			return list.get(0);
	}

	@Override
	public <T> T findById(Class<T> clazz, String modelId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DocumentModel> List<T> findByProperty(String modelName, String propertyName, Object value) {
		Map<String, Object> properties = new HashMap<>();
		properties.put(propertyName, value);
		return findByProperties(modelName, properties);
	}

	@Override
	public <T> List<T> findByProperty(Class<T> clazz, String propertyName, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends DocumentModel> List<T> findByProperties(String modelName, Map<String, Object> properties) {
		List<T> list = new ArrayList<>();
		BoolQueryBuilder queryBuilder = null;
		if (properties != null) {
			queryBuilder = QueryBuilders.boolQuery();
			for (String property : properties.keySet()) {
				queryBuilder.must(QueryBuilders.matchQuery(property, properties.get(property)));
			}
		}

		SearchRequestBuilder requestBuilder = connection.getClient().prepareSearch(connection.getIndex())
				.setTypes(modelName.toLowerCase()).setSearchType(SearchType.QUERY_AND_FETCH);
		if (queryBuilder != null) {
			requestBuilder.setQuery(queryBuilder);
		}
		SearchResponse response = requestBuilder.execute().actionGet();
		if (response.getHits().getTotalHits() > 0) {
			ModelResource modelResource = (ModelResource) resourceRegistry.getResource(modelName);
			Class<?> clazz = null;

			if (modelResource.getModelClass() != null) {
				try {
					clazz = context.getBundle(modelResource.getBundleId()).loadClass(modelResource.getModelClass());

					for (SearchHit hit : response.getHits().getHits()) {
						T model = null;
						if (clazz == null) {
							model = (T) new PersistedModelBase();
						} else {
							model = (T) clazz.newInstance();
						}
						copy(model, hit.getSource(), modelResource.getProperties());
						model.setId(hit.getId());
						model.setModelId(hit.getId());
						list.add(model);
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(modelResource);
				}
			}
		}
		return list;
	}

	@Override
	public <T> List<T> findByProperties(Class<T> clazz, Map<String, Object> properties) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean modelExists(String modelName) {
		String[] indices = { connection.getIndex() };
		TypesExistsRequest req = new TypesExistsRequest(indices, modelName.toLowerCase());
		return connection.getClient().admin().indices().typesExists(req).actionGet().isExists();
	}

	@Override
	public void addModel(String modelName) {
		String model = modelName.toLowerCase();
		try {
			if (!modelExists(modelName)) {
				ModelResource modelResource = (ModelResource) resourceRegistry.getResource(modelName);
				PutMappingRequest request = new PutMappingRequest(connection.getIndex()).type(model)
						.source(buildSourceMapping(modelResource.getProperties()));
				connection.getClient().admin().indices().putMapping(request).actionGet();
				logService.log(LogService.LOG_INFO, "Created new index type: " + model);
			}
		} catch (Exception e) {
			logService.log(LogService.LOG_ERROR, e.getMessage(), e);
		}
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createQuery(CriteriaUpdate updateQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Query createQuery(CriteriaDelete deleteQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceService begin() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceService commit() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceService rollback() {
		// TODO Auto-generated method stub
		return null;
	}

	private <T extends DocumentModel> void persist(String modelName, T model) {
		String newId = model.getModelId();
		if (newId == null) {
			newId = IdGenerator.newId();
			model.setModelId(newId);
			model.setId(newId);
		}

		UpdateRequest updateRequest = connection.getClient()
				.prepareUpdate(connection.getIndex(), modelName.toLowerCase(), newId).setDoc(model.getProperties())
				.setUpsert(model.getProperties()).request();
		UpdateResponse res = connection.getClient().update(updateRequest).actionGet();
		System.out.println(res.getGetResult().getId());
	}

	private <T extends DocumentModel> void copy(T model, Map<String, Object> fields, Map<String, Property> typeMap) {
		for (String name : fields.keySet()) {
			if (typeMap.get(name).getType().equals("java.util.Date")) {//TODO: Date or new Date
				try {
					model.setProperty(name, Date.from(Instant.parse(fields.get(name).toString())));
				} catch (DateTimeParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				model.setProperty(name, fields.get(name));
			}
		}
	}

	private XContentBuilder buildSourceMapping(Map<String, Property> properties) {
		XContentBuilder mapping = null;
		try {
			mapping = XContentFactory.jsonBuilder();
			mapping.startObject();
			buildProperties(mapping, properties);
			mapping.endObject();
		} catch (IOException e) {
			logService.log(LogService.LOG_ERROR, e.getMessage(), e);
		}
		return mapping;
	}

	private void buildProperties(XContentBuilder mapping, Map<String, Property> properties) throws IOException {
		mapping.startObject("properties");
		for (Property property : properties.values()) {
			String type = getElasticsearchType(property.getType());
			mapping.startObject(property.getName()).field("type", type);
			if (type.equals("object")) {
				ModelResource resource = (ModelResource) resourceRegistry.getResource(property.getType());
				buildProperties(mapping, resource.getProperties());
			}
			mapping.endObject();
		}
		mapping.endObject();
	}

	private String getElasticsearchType(String type) {
		String elasticsearchType = typeMapper.getElasticsearchType(type);
		if (elasticsearchType == null) {
			// TODO: WARN what if weblet resource name matches?
			if (resourceRegistry.getResource(type) != null) {
				elasticsearchType = "nested";
			} else {
				elasticsearchType = "object";
			}
		}
		return elasticsearchType;
	}

}
