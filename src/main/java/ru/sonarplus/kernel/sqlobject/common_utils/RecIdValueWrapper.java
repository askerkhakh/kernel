package ru.sonarplus.kernel.sqlobject.common_utils;

import java.util.ArrayList;
import java.util.List;

public class RecIdValueWrapper {

    private List<Object> values = new ArrayList<>();

	public RecIdValueWrapper() {}
	
	public int count() {
		return values.size();
	}
	
	public boolean isComposite() {
		return count() > 1;
	}

	public boolean isContainsValue() {
		return count() > 0 ;
	}

	public Object getValue(int index) {
		return values.get(index);
	}
	public void add(Object value) {
		values.add(value);
	}

}
