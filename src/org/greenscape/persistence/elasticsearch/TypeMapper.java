package org.greenscape.persistence.elasticsearch;

import java.util.HashMap;
import java.util.Map;

public class TypeMapper {
	private Map<String, String> mappings;

	public TypeMapper() {
		mappings = new HashMap<>();
		mappings.put("java.lang.Boolean", "boolean");
		mappings.put("java.lang.Double", "double");
		mappings.put("java.lang.Float", "float");
		mappings.put("java.lang.Integer", "integer");
		mappings.put("java.lang.Long", "long");
		mappings.put("java.lang.String", "string");
		mappings.put("java.util.Date", "date");
	}

	public String getElasticsearchType(String javaType) {
		return mappings.get(javaType);
	}
}
