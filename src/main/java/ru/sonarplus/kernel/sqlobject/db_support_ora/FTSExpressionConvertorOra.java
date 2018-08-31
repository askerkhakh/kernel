package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.db_support.FullTextEngine;
import ru.sonarplus.kernel.sqlobject.db_support_ora.FTSStringParser.*;

import java.util.ArrayList;
import java.util.List;

public class FTSExpressionConvertorOra {
	protected static String FTS_WORD_DELIMITER = "OR";
	FullTextEngine ftsEngine;
	List<String> wordList = new ArrayList<String>();
			
	public FTSExpressionConvertorOra(FullTextEngine engine) {
		ftsEngine = engine;
	}
	
	protected String getResultString() {
		return String.join(" ", wordList);
	}
	
	protected void addWord(String word) {
		String localWord = ftsEngine == FullTextEngine.ORACLE_TEXT ? word.replace('*', '%') : word;
		wordList.add(localWord);
	}
	
	protected String getOperatorValue(FTSOperator operator) {
		switch (operator) {
		case OR:
			return "OR";
		case AND:
			return "AND";
		case NOT:
			return "NOT";
		default:
			return "";
		}
	}
	
	protected void addOperation(FTSExpression expression) {
		if ((expression.operator != null) && (expression.operator != FTSOperator.NONE)) {
			addWord(getOperatorValue(expression.operator));
		}
	}
	
	protected void getGroupOfWordsValue(GroupOfWords group) {
		for (int i = 0; i < group.items.size(); i++) {
			if (i > 0) {
				addWord(FTS_WORD_DELIMITER);
			}
			addWord(group.items.get(i));
		}
	}
	
	protected void getPhrasePartValue(SeparatePhrasePart phrasePart) {
		Preconditions.checkArgument(phrasePart.groupingSymbol != FTSGroupingSymbol.DOUBLE_QUOTES);
		if (phrasePart.groupingSymbol == FTSGroupingSymbol.PARENTHESIS) {
			Preconditions.checkArgument(phrasePart.value instanceof FTSExpression);
			addWord("(");
			getFTSExpressionValue((FTSExpression) phrasePart.value);
			addWord(")");
		}
		else {
			Preconditions.checkArgument(phrasePart.value instanceof GroupOfWords);
			getGroupOfWordsValue((GroupOfWords) phrasePart.value);
		}
	}
	
	protected void getPhraseValueByOperand(Phrase phrase) {
		if (phrase.operand1 != null) {
			Preconditions.checkArgument(phrase.operand1 instanceof PhrasePartsGroup);
			PhrasePartsGroup phraseParts = (PhrasePartsGroup) phrase.operand1;
			for (int i = 0; i < phraseParts.getSize(); i++) {
				if (i > 0) {
					addWord(FTS_WORD_DELIMITER);
				}
				getPhrasePartValue(phraseParts.getSeparatePart(i));
			}
		}
		
		addOperation(phrase);
		
		if (phrase.operand2 != null) {
			Preconditions.checkArgument(phrase.operand2 instanceof Phrase);
			getPhraseValueByOperand((Phrase) phrase.operand2);
		}
	}
	
	protected void getFTSExpressionValue(FTSExpression ftsExpression) {
		// Первый операнд всегда типа Phrase
		if (ftsExpression.operand1 != null) {
			Preconditions.checkArgument(ftsExpression.operand1 instanceof Phrase);
			getPhraseValueByOperand((Phrase) ftsExpression.operand1);
		}
		addOperation(ftsExpression);
		
		// А второй всегда FTSExpression
		if (ftsExpression.operand2 != null) {
			Preconditions.checkArgument(ftsExpression.operand2 instanceof FTSExpression);
			getFTSExpressionValue((FTSExpression) ftsExpression.operand2);
		}
	}
	
	public String convertFTSExpression(FTSExpression ftsExpression) {
		getFTSExpressionValue(ftsExpression);
		return getResultString();
	}

}
