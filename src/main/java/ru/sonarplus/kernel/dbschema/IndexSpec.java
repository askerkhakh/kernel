package ru.sonarplus.kernel.dbschema;

import java.util.ArrayList;
import java.util.List;

public class IndexSpec extends DbdItemWithAttributes{
	public String name;
	public IndexType indexType;
	public List<IndexItemSpec> items = new ArrayList<IndexItemSpec>();

	public IndexSpec() {
		// TODO Auto-generated constructor stub
	}
	
	public String getName() {
		return name;
	}
	
	public IndexType getIndexType() {
		return indexType;
	}
	
	public int getItemsCount() {
		return items.size();
	}
	
	public IndexItemSpec getItem(int index) {
		return items.get(index);
	}

}
