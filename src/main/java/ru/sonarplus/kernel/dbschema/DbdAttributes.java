package ru.sonarplus.kernel.dbschema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DbdAttributes {
	public Set<String> attributesSet = new HashSet<String>();
	public Map<String, Object> attributesMap = new HashMap<String, Object>();

	public DbdAttributes() {
		// TODO Auto-generated constructor stub
	}

	public void addAttribute(String name) {
		attributesSet.add(name);
	}

	public void addAttribute(String name, Object value) {
		addAttribute(name);
		attributesMap.put(name, value);
	}
	
	public boolean hasAttribute(String name) {
		return attributesSet.contains(name);
	}
	
	public Object getAttributeValue(String name) {
		return attributesMap.get(name);
	}

}
