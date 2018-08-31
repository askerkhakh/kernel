package ru.sonarplus.kernel.dbschema;

import java.util.HashMap;
import java.util.Map;

public enum FieldTypeId {
	tid_UNKNOWN ("UNKNOWN"), 
	tid_STRING ("STRING"), 
	tid_SMALLINT ("SMALLINT"),      
	tid_INTEGER ("INTEGER"),       
	tid_WORD ("WORD"),          
	tid_BOOLEAN ("BOOLEAN"),       
	tid_FLOAT ("FLOAT"),         
	tid_FMTBCD ("FMTBCD"),
	tid_DATE ("DATE"),          
	tid_TIME ("TIME"),          
	tid_DATETIME ("DATETIME"),      
	tid_BLOB ("BLOB"),          
	tid_MEMO ("MEMO"),          
	tid_GRAPHIC ("GRAPHIC"),       
	tid_LARGEINT ("LARGEINT"),      
	tid_CODE ("CODE"),          
	tid_BYTE ("BYTE");    

	private String text;

	FieldTypeId (String text) {
		this.text = text;  
	}

	static final Map<String, FieldTypeId> TYPES_MAP = new HashMap() {
		{
			for (FieldTypeId item: FieldTypeId.values()) {
				put(item.text, item);
			}
		}
	};

	public static FieldTypeId fromString(String text) {
		return TYPES_MAP.get(text);
	}
	
    public String toString() {
		  return text;
    }

}
