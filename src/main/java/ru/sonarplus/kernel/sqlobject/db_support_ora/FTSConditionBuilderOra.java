package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.db_support.FullTextEngine;
import ru.sonarplus.kernel.sqlobject.db_support_ora.FTSStringParser.*;
import ru.sonarplus.kernel.sqlobject.expressions.Expr;
import ru.sonarplus.kernel.sqlobject.objects.*;

public class FTSConditionBuilderOra {
	ColumnExpression field;
	FieldTypeId valueType;
	FullTextEngine ftsEngine;
	boolean sortByResult;

	public FTSConditionBuilderOra(FullTextEngine engine, ColumnExpression field,
			FieldTypeId valueType, boolean sortByResult) {
		ftsEngine = engine;
		this.field = field;
		this.valueType = valueType;
		this.sortByResult = sortByResult;
	}
	
	protected static boolean isLetterOrDigit(char ch) {
		return Character.isLetter(ch) || 
				Character.UnicodeBlock.of(ch).equals(Character.UnicodeBlock.CYRILLIC) ||
				Character.isDigit(ch);
	}
	
	protected String getValueExceptPunctuationMarks(String precisePhrase) {
		StringBuilder result = new StringBuilder(); 
	    // Исключаем все не буквы и не цифры
		for (int i = 0; i < precisePhrase.length(); i++) {
			char ch = precisePhrase.charAt(i);
			if (isLetterOrDigit(ch)  || (ch == ' ')) {
				result.append(ch);
			}
			else {
				result.append(' ');
			}
		}
		return result.toString();
	}
	
	protected boolean isSingleWord(String phrase) {
		for (int i = 0; i < phrase.length(); i++) {
			char ch = phrase.charAt(i);
			if (!isLetterOrDigit(ch)) {
				return false;
			}
		}
		return true;
	}
	
	protected static String getFTSFunction(FullTextEngine ftsEngine) {
		switch (ftsEngine) {
		case ORACLE_TEXT:
			return "contains";
		case LUCENE:
			return "lcontains";
		default:
			Preconditions.checkArgument(false, "Указанный способ полнотекстового поиска %s не поддержан",
					ftsEngine.toString());
			
			return null;
		}
	}
	
	public static PredicateComparison getContainsComparison(FullTextEngine ftsEngine,
			ColumnExpression columnExpression,
			String ftsCondition,
			FieldTypeId valueType,
			boolean sortByResult)
			throws SqlObjectException {
	    /* поле, шаблон и уникальная числовая метка,
	        используемая oracle для идентификации вычисленного в [l]contains ранга совпадения.
	        Эта числовая метка в виде [l]score(N) может затем использоваться затем разделах SELECT/ORDER BY...
	        Мы пока включаем при необходимости этот [l]score(N) только в раздел сортировок. */
		String functionParams = sortByResult ? "(??, ??, ??)" : "(??, ??)";
		/* выражение с безымянными аргументами:
		    первый - проверяемое выражение
		    второй - шаблон */
		
		ColumnExpression ftsExpression = new Expression(null, getFTSFunction(ftsEngine)+functionParams, true);
	    // аргумент - проверяемое выражение
		ftsExpression.insertItem(columnExpression);
		
	    // аргумент - строка шаблона
	    // Если полнотекстовый поиск идет по бинарному полю, то для задания значения
	    // будем использовать тип MEMO, т.к. в любом случае мы сведём все к тому, чтобы
	    // все операции поиска производились с текстовыми данными, получеными на основе
	    // бинарных 
		FieldTypeId localValueType = valueType == FieldTypeId.tid_BLOB ? FieldTypeId.tid_MEMO : valueType;
		ftsExpression.insertItem( new ValueConst(ftsCondition, localValueType));
		
		// аргумент - числовая метка
		if (sortByResult) {
			OraFTSMarker marker = new OraFTSMarker(ftsExpression);
			marker.sortByScore = true;
		}
		
		PredicateComparison result = new PredicateComparison(PredicateComparison.ComparisonOperation.GREAT);
		result.setLeft(ftsExpression);
		result.setRight(new ValueConst(0, FieldTypeId.tid_INTEGER));
		return result;
	}
	
	protected PredicateComparison getContainsCondition(String word) {
		String localWord = ftsEngine == FullTextEngine.ORACLE_TEXT ? word.replace('*', '%') : word;
		return getContainsComparison(ftsEngine, (ColumnExpression) field.getClone(), localWord, valueType, sortByResult);
	}
	
	protected Conditions getPrecisePhraseCondition(GroupOfWords groupOfWords) {
		Preconditions.checkState(field instanceof QualifiedField);
		Conditions result = new Conditions(null, Conditions.BooleanOp.AND);
		String precisePhrase = groupOfWords.items.get(0);
		
		// Формируем выражение contains
		boolean isOracleText = ftsEngine == FullTextEngine.ORACLE_TEXT;  
		String searchText =  (isOracleText ? "{" : "\"") +
				getValueExceptPunctuationMarks(precisePhrase) + 
				(isOracleText ? "}" : "\"");
		result.addCondition(getContainsCondition(searchText));
		
	    /* Проверяем из чего состоит поисковая фраза. Если она представляет собой
	        одно слово без разделителей, то для выполнения полнотекстового поиска точной
	        фразы строить выражение с оператором LIKE не требуется.
	        Проверка выполнена здесь, поскольку при использовании расширенного режима
	        полнотекстового поиска может встретится несколько операторов поиска точной
	        фразы. В этом случае нужно будет отдельно обработать каждый из операторов. */
		if (isSingleWord(precisePhrase)) {
			return result;
		}
		
	    // Формируем выражение like
		QualifiedName qName = ((QualifiedField) field).getQName();
		Expression valueExpr = new Expression(Expr.exprUpper("??"), false);
		valueExpr.insertItem(new ValueConst("%"+precisePhrase+"%", FieldTypeId.tid_STRING));
	    
		// В случае, если поиск идет по бинарному полю, его имя будет заменено на вызов
	    // функции, возвращающей текстовые данные из бинарных
		PredicateLike predicateLike = new PredicateLike(result, "@");
		predicateLike.setLeft(new Expression(
				Expr.exprUpper(Expr.exprBinDataToText(qName.alias, qName.name)), false));
		predicateLike.setRight(valueExpr);
		return result;
		
	}
	
	protected Conditions getGroupOfWordsConditions(GroupOfWords groupOfWords) {
		Conditions result = new Conditions(Conditions.BooleanOp.OR);
		for (int i = 0; i < groupOfWords.items.size(); i++) {
			result.addCondition(getContainsCondition(groupOfWords.items.get(i)));
		}
		return result;
	}
	
	protected Conditions getPhrasePartCondition(SeparatePhrasePart phrasePart) {
		switch (phrasePart.groupingSymbol) {
		case PARENTHESIS:
			return buildFTSCondition((FTSExpression) phrasePart.value);
		case DOUBLE_QUOTES:
			return getPrecisePhraseCondition((GroupOfWords) phrasePart.value);
		default:
			return getGroupOfWordsConditions((GroupOfWords) phrasePart.value);
		}
		
	}
	
	protected Conditions getPhrasePartsConditions(Phrase phrase) {
		if (phrase.operand1 != null) {
			PhrasePartsGroup phraseParts = (PhrasePartsGroup) phrase.operand1;
			Conditions firstCondition = null;
			if (phraseParts.getSize() > 1) {
				firstCondition = new Conditions(null, Conditions.BooleanOp.OR);
				for (int i = 0; i < phraseParts.getSize(); i++) {
					firstCondition.addCondition(getPhrasePartCondition(phraseParts.getSeparatePart(i)));
				}
			}
			else {
				firstCondition = getPhrasePartCondition(phraseParts.getSeparatePart(0));
			}
			if (phrase.operand2 != null) {
				Preconditions.checkArgument(phrase.operand2 instanceof Phrase);
				Conditions result = new Conditions(Conditions.BooleanOp.AND);
				result.addCondition(firstCondition);
				Conditions secondConditions = getPhrasePartsConditions((Phrase)phrase.operand2);
				if (phrase.operator == FTSOperator.NOT) {
					secondConditions.not = true;
				}
				result.addCondition(secondConditions);
				return result;
			}
			else {
				return firstCondition;
			}
		}
		return null;
	}
	
	public Conditions buildFTSCondition(FTSExpression expression) {
		if (expression.operand1 != null) {
			// Первый операнд всегда типа Phrase
			Conditions firstCondition = getPhrasePartsConditions((Phrase) expression.operand1);
			if (expression.operand2 != null) {
				Preconditions.checkArgument(expression.operand2 instanceof FTSExpression);
				Conditions result = new Conditions();
				result.addCondition(firstCondition);
				// По построению промежуточного представления здесь может быть только OR
				if (expression.operator == FTSOperator.OR) {
					result.booleanOp = Conditions.BooleanOp.OR;
					Conditions secondCondition = buildFTSCondition((FTSExpression)expression.operand2);
					result.addCondition(secondCondition);
				}
				return result;
			}
			else {
				return firstCondition;
			}
		}
		return null;
	}

}
