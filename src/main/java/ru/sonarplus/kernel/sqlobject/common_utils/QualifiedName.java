package ru.sonarplus.kernel.sqlobject.common_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public class QualifiedName {
	public static final char LEFT_BRACE_CHAR = '[';
	public static final char RIGHT_BRACE_CHAR = ']';
	
	public String alias;
	public String name;

	public QualifiedName(String alias, String name) {
		this.name = name;
		this.alias = alias;
	}
	
	public static String formQualifiedNameStringEx(String alias, String fieldName,
		    char leftBrace, char rightBrace,
			String defaultAlias) {
        String aliasValue = (StringUtils.isEmpty(alias) ? defaultAlias: alias);
		if (StringUtils.isEmpty(aliasValue))
			return fieldName;

		if (!aliasValue.equals(defaultAlias)) {
			String result = leftBrace + aliasValue + rightBrace;
			if (!StringUtils.isEmpty(fieldName)) {
				if (result.equals("") && (fieldName.charAt(0) == leftBrace) &&
						(fieldName.charAt(fieldName.length()-1) == rightBrace)) {
					result = "" +leftBrace + rightBrace;
				}
				result += "."+fieldName;
			}
			return result;
		}
		return "";
	}
	
	
	public static String formQualifiedNameString(String alias, String fieldName,
			String defaultAlias) {
		return formQualifiedNameStringEx(alias, fieldName, 
				LEFT_BRACE_CHAR, RIGHT_BRACE_CHAR, defaultAlias);
	}

	public static String formQualifiedNameString(String alias, String fieldName) {
		return formQualifiedNameString(alias, fieldName, "");
	}
	
	public static QualifiedName stringToQualifiedNameEx(String qualifiedNameString, char leftBrace, char rightBrace) {
		QualifiedName result = new QualifiedName("", "");
		int p = 0;
		if ((qualifiedNameString.length() > 0) && (qualifiedNameString.charAt(0) == leftBrace)) {
			p = qualifiedNameString.indexOf(rightBrace, 1);
			if (p > 0) {
				result.alias = qualifiedNameString.substring(1, p);
				if (p == qualifiedNameString.length() - 1) {
					p++;
				}
				else if (qualifiedNameString.charAt(p+1) != '.') {
					Preconditions.checkArgument(false, 
							"Неверная запись полного имени поля (%s) отсутствует точка после имени таблицы.",
							qualifiedNameString);
				}
				else {
					p += 2;
				}
			}
		}
		result.name = qualifiedNameString.substring(p);
		return result;
	}

	public static QualifiedName stringToQualifiedName(String qualifiedNameString) {
		return stringToQualifiedNameEx(qualifiedNameString, LEFT_BRACE_CHAR, RIGHT_BRACE_CHAR); 
	}
	
	public String qualifiedNameToString() {
		return formQualifiedNameString(alias, name);
	}

	@Override
	public String toString() {
		return formQualifiedNameString(alias, name);
	}
}
