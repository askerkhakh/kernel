package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static ru.sonarplus.kernel.sqlobject.db_support_ora.FTSStringParser.SplitState.*;

public class FTSStringParser {
	protected static String END_OF_LINE_TOKEN = "-1";
	
	public boolean strСontainQuotes;
	public int position;
	public List<String> lexemList = new ArrayList<String>();

	public FTSStringParser(String searchString) {
		splitStringIntoLexem(searchString);
	}
	
	protected static boolean isIndentSymbol(char ch) {
		return Character.isLetter(ch) || 
				Character.isDigit(ch) ||
				(ch == '*') ||
				Character.UnicodeBlock.of(ch).
				equals(Character.UnicodeBlock.CYRILLIC);
	}

	protected enum SplitState {DEFAULT, ESCAPED_QUOTE, NOT_QUOTED_TEXT, QUOTED_TEXT}
	protected class SplitContext {
	    public SplitState state = DEFAULT;
	    public StringBuilder token = new StringBuilder();
	    public char previousChar = 0;
    }

	protected void addAndResetToken(StringBuilder token) {
	    if (token.length() != 0) {
	        String newToken = token.toString();
            // Пропускаем звездочку, т.к. она не имеет смысла как самостоятельное слово
	        if (!newToken.equals("*"))
	            lexemList.add(newToken);
	        token.setLength(0);
        }
    }

    protected void splitStringIntoLexem(String searchString) {
	    SplitState state = DEFAULT;
	    char currentChar, previousChar = 0;
        StringBuilder token = new StringBuilder();
        for (int i = 0; i < searchString.length(); i++) {
            currentChar = searchString.charAt(i);
            switch (state) {
                case QUOTED_TEXT:
                    if (currentChar == '\\')
                        // возможно, что далее экранированная кавычка, символ пока пропустим
                        state = ESCAPED_QUOTE;
                    else if (currentChar == '"'){
                        // завершение квотированного текста
                        addAndResetToken(token);
                        lexemList.add("\"");
                        state = DEFAULT;
                    }
                    else
                        // внутри кавычек "" собираем все символы
                        token.append(currentChar);
                    break;

                case ESCAPED_QUOTE:
                    if (currentChar != '"')
                        // экранируем только ". в противном случае добавляем и \ и текущий символ
                        token.append('\\');
                    token.append(currentChar);
                    // т.к. предположение об экранированном символе возникает в квотированном тексте - вернём состояние
                    state = QUOTED_TEXT;
                    break;

                case NOT_QUOTED_TEXT:
                    if (isIndentSymbol(currentChar))
                        token.append(currentChar);
                    else {
                        addAndResetToken(token);
                        switch (currentChar) {
                            case '(':
                            case ')':
                                addAndResetToken(token);
                                state = DEFAULT;
                                break;

                            case '|':
                            case '&':
                            case '-':
                                if (previousChar == ' ') {
                                    token.append(currentChar);
                                    addAndResetToken(token);
                                }
                                else {
                                    // иначе это просто разделитель слова. его пропустим
                                }
                                state = DEFAULT;
                                break;

                            //case '"':
                            //    break;
                        }
                    }
                    break;

                default: // case DEFAULT
                    if (isIndentSymbol(currentChar)) { // ['A'..'Z', 'a'..'z', 'А'..'Я', 'Ё', 'а'..'я', 'ё', '0'..'9', '*']
                        addAndResetToken(token);
                        token.append(currentChar);
                        state = NOT_QUOTED_TEXT;
                    }
                    else
                        switch (currentChar) {
                            case '"':
                                // начало квотированного текста
                                addAndResetToken(token);
                                lexemList.add("\"");
                                strСontainQuotes = true;
                                state = QUOTED_TEXT;
                                break;

                            case '(':
                            case ')':
                                addAndResetToken(token);
                                state = DEFAULT;
                                break;

                            case '|':
                            case '&':
                            case '-':
                                if (previousChar == ' ') {
                                    token.append(currentChar);
                                    addAndResetToken(token);
                                }
                                else {
                                    // иначе это просто разделитель слова. его пропустим
                                }
                                state = DEFAULT;
                                break;

                        }
            }
            previousChar = currentChar;
        }
        Preconditions.checkArgument(!(state == QUOTED_TEXT || state == ESCAPED_QUOTE));
        addAndResetToken(token);
    }

	protected boolean isEndOfLexemList() {
		return lexemList.size() <= position;
	}
	
	protected String getToken() {
		if (!isEndOfLexemList()) {
			return lexemList.get(position);
		}
		else {
			return END_OF_LINE_TOKEN;
		}		
	}
	
	protected void nextToken() {
		position++;
	}
	
	protected boolean skipToken(char value) {
		boolean result = getToken().equals(""+value);
		if (result) {
			nextToken();
		}
		return result;
	}
	
	protected static boolean tokenIsWord(String token) {
		for (int i = 0 ; i < token.length(); i++) {
			if (!isIndentSymbol(token.charAt(i))) {
				return false;
			}
		}
		return true;
		
	}
	
	protected void raiseWrongSearchStringError(String exceptedToken, String token) {
		String errToken = token.equals(END_OF_LINE_TOKEN) ? "конец строки" : token;
		Preconditions.checkState(false, 
				"Неверно сформированная поисковая строка. Ожидалось: ''%s'', получено: ''%s''.",
				exceptedToken, errToken);
		
	}
	
	protected void setPhraseValue(GroupOfWords orerand, boolean isWordInQuotes) {
		//<Group of word> ::= <FTSWord> {<Group of word>}
		if (isWordInQuotes) {
			if (!isEndOfLexemList()) {
	            // Добавляем лексему, содержащую в себе выражение для точного поиска.
		        // Перед добавлением избавляемся от экранирования кавычек
				orerand.addWord(getToken().replaceAll("\\\"", "\""));
				nextToken();
			}
		}
		else {
			orerand.addWord(getToken());
			nextToken();
			if (!isEndOfLexemList() && tokenIsWord(getToken())) {
				setPhraseValue(orerand, false);
			}
		}
	}
	
	protected boolean tokenIsPartOfPhrase(String token) {
		return token.equals("\"") || token.equals("(") || tokenIsWord(token);
	}
	
	protected SeparatePhrasePart getSeparatePhrasePart() {
		//<Separate phrase part> ::= '('<Expr>')'|'"'<Group of word>'"'| <Group of word>
		SeparatePhrasePart result = new SeparatePhrasePart();
		if (skipToken('(')) {
			result.groupingSymbol = FTSGroupingSymbol.PARENTHESIS;
			result.value = getFTSExpression();
			if (!skipToken(')')) {
				raiseWrongSearchStringError(")", getToken());
			}
		}
		else if (skipToken('"')) {
			result.groupingSymbol = FTSGroupingSymbol.DOUBLE_QUOTES;
			GroupOfWords groupOfWords = new GroupOfWords(); 
			result.value = groupOfWords;
			setPhraseValue(groupOfWords, true);
			if (!skipToken('"')) {
				raiseWrongSearchStringError("\"", getToken());
			}
		}
		else {
			if (tokenIsWord(getToken())) {
				GroupOfWords groupOfWords = new GroupOfWords(); 
				result.value = groupOfWords;
				setPhraseValue(groupOfWords, false);
				
			}
		}
		return result;
	}
	
	protected PhrasePartsGroup getPhraseParts() {
		PhrasePartsGroup result = new PhrasePartsGroup();
		while (tokenIsPartOfPhrase(getToken())) {
			SeparatePhrasePart separatePart = getSeparatePhrasePart();
			if (separatePart.value != null) {
				result.addSeparatePart(separatePart);
			}
		}
		return result;
	}

	protected Phrase getPhrase() {
		  //<Phrase> ::= <Separate phrase parts> <Operation2> <Phrase>
		Phrase result = new Phrase();
		PhrasePartsGroup phrasePartsGroup = getPhraseParts();
		Preconditions.checkArgument(phrasePartsGroup.getSize() > 0);
		
		result.operand1 = phrasePartsGroup;
		if (skipToken('&')) {
			result.operator = FTSOperator.AND; 
		}
		else if (skipToken('-')) {
			result.operator = FTSOperator.NOT;
		}
		
		if ((result.operator == FTSOperator.AND) || (result.operator == FTSOperator.NOT)) {
			result.operand2 = getPhrase();
		}
		return result;
		
	}
	
	protected static class FTSOperand {
		
	}
	
	protected enum FTSOperator {
		NONE, OR, AND, NOT
	}
	
	protected enum FTSGroupingSymbol {
		NONE, PARENTHESIS, DOUBLE_QUOTES
	}
	
	protected static class FTSExpression extends FTSOperand {
		public FTSOperator operator;
		public FTSOperand operand1;
		public FTSOperand operand2;
		public boolean isСontainQuotes;
	}
	
	protected static class Phrase extends FTSExpression {
		
	}
	
	protected static class SeparatePhrasePart extends FTSOperand {
		public FTSOperand value;
		public FTSGroupingSymbol groupingSymbol;
	}
	
	protected static class PhrasePartsGroup extends FTSOperand {
		public List<SeparatePhrasePart> items = new ArrayList<SeparatePhrasePart>();
		
		protected SeparatePhrasePart getSeparatePart(int index) {
			return items.get(index);
		}
		
		protected void addSeparatePart(SeparatePhrasePart separatePart) {
			items.add(separatePart);
		}
		
		protected int getSize() {
			return items.size();
		}
	}
	
	protected static class GroupOfWords extends FTSOperand {
		public List<String> items = new ArrayList<String>();
		
		public void addWord(String word) {
			items.add(word);
		}
	}
	
	
	protected FTSExpression getFTSExpression() {
  	    //<Expr> ::= <Phrase> <Operation1> <Expr>		
		FTSExpression result = new FTSExpression();
		result.operand1 = getPhrase();
		if (skipToken('|')) {
			result.operator = FTSOperator.OR;
			result.operand2 = getFTSExpression();
		}
		return result;
	}
	
	public static FTSExpression parseFTSString(String searchString) {
		FTSStringParser stringParser = new FTSStringParser(searchString);
		FTSExpression result = stringParser.getFTSExpression();
		result.isСontainQuotes = stringParser.strСontainQuotes;
		return result;
	}

}
