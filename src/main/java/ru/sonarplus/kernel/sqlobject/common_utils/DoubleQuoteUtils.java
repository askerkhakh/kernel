package ru.sonarplus.kernel.sqlobject.common_utils;

import org.apache.commons.lang.StringUtils;

public class DoubleQuoteUtils {
	public static final String QUOTE = "\"";

	protected static boolean isNeedQuote(String value) {
		if (StringUtils.isEmpty(value)) {
			return false;
		}
		
		char first = value.charAt(0);
		if (!(Character.isLetter(first) || 
		  Character.UnicodeBlock.of(first).equals(Character.UnicodeBlock.CYRILLIC))) {
			return true;
		}
		
		for (int i = 1; i < value.length(); i++) {
			char item = value.charAt(i);
			if (!(Character.isLetterOrDigit(item) ||
				Character.UnicodeBlock.of(item).equals(
						Character.UnicodeBlock.CYRILLIC) ||
				(item == '_') || (item == '#') || (item == '$'))) {
				return true;
				
			}
			
		}
		return false;

	}
	
	protected static boolean isDoubleQuoted(String source) {
		return source.startsWith(QUOTE) && source.endsWith(QUOTE);
	}

    public static class DoubleQuoteException extends Exception {
        public DoubleQuoteException(String message) {
            super(message);
        }
    }

    protected static String internalDoubleQuoteIdentifier(String source, 
            boolean isNeedForceQuote) {
        String result = source;
        if (!isDoubleQuoted(result) && (isNeedForceQuote || isNeedQuote(result))) {
            if (result.contains(QUOTE))
                new DoubleQuoteException(String.format("Оборачиваемая в %s строка <%s> содержит символ '%s'", QUOTE + QUOTE, result, QUOTE));
            result = QUOTE +result + QUOTE;
        }
        return result;
    }

	public static String doubleQuoteIdentifierIfNeed(String source) {
		return internalDoubleQuoteIdentifier(source, false);
	}
	
	public static String doubleDequoteIdentifier(String source) {
		String result = source;
		if (isDoubleQuoted(result)) {
			result = result.substring(1, result.length()-1);
		}
		return result;
	}
}
