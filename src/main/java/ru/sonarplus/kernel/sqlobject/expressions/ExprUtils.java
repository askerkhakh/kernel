package ru.sonarplus.kernel.sqlobject.expressions;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.RenamingDict;
import ru.sonarplus.kernel.sqlobject.objects.Expression;

import java.util.ArrayList;
import java.util.List;

public class ExprUtils {

	public ExprUtils() {
	}

    public static final String NOT_CORRESPONDING_UNNAMED_ARG_REFS_WITH_EXPR_CHILDS =
            "В выражении '%s' количество безымянных ссылок на аргументы не соответствует количеству подчинённых аргументов выражения";
    public static final String MIXED_NAMED_AND_UNNAMED_REFS =
            "В выражении '%s' смешаны именованые и безымянные ссылки";
    public static final String EXPECTED_UNNAMED_REFS =
            "В выражении '%s' ожидаются только безымянные ссылки";
    public static final String NOT_FOUND_ARGUMENT_OF_EXPRESSION_BY_REF =
            "В выражении '%s' по ссылке  '%s' не найден аргумент выражения";
    public static final String UNNAMED_REFS_COUNT_MORE_THAN_ARGUMENTS_COUNT =
            "В выражении '%s' количество ссылок превышает количество аргументов выражения";
    public static final String EXPR_HAS_ARGUMENTS_THAT_NOT_CORRESPONDS_WITH_REFS =
            "В выражении '%s' остались аргументы, не соотнесённые со ссылками на них из выражения";

    protected static QualifiedName[] extractQNames(String expr, String tag) {
        String localTemplate = ExprConsts.EXPR_BEGIN + tag + ExprConsts.DELIMITER;
        int pEnd = 0;
        List<QualifiedName> tempList = new ArrayList<QualifiedName>();
        while (true) {
            int pBegin = expr.indexOf(localTemplate, pEnd);
            if (pBegin >= 0) {
                pEnd = expr.indexOf(ExprConsts.EXPR_END, pBegin);
                Preconditions.checkArgument(pEnd >= 0);
                String sQName = expr.substring(pBegin + localTemplate.length(), pEnd);
                Preconditions.checkState(sQName.length() > 0);
                tempList.add(QualifiedName.stringToQualifiedName(sQName));
            }
            else {
                break;
            }
        }
        return tempList.toArray(new QualifiedName[tempList.size()]);
    }

    /** Извлекает из переданной строки перечень квалифицированных имён, включённых в строку
     * с помощью Expr.exprQRName()
     *
     * @param expr
     * @return
     */
    public static QualifiedName[] exprExtractQRNames(String expr) {
        return extractQNames(expr, ExprConsts.QRNAME);
    }

    public static QualifiedName[] exprExtractQNames(String expr) {
        return extractQNames(expr, ExprConsts.QNAME);
    }

	public static String[] exprExtractBinToTextFields(String expr) {
		QualifiedName[] qNames = extractQNames(expr, ExprConsts.FUNCTION_BINDATA_TOTEXT);
		String[] result = new String[qNames.length];
		for (int i = 0; i < qNames.length; i++) {
			result[i] = qNames[i].qualifiedNameToString(); 
		}
		return result;
	}

    /** Извлекает из переданной строки перечень квалифицированных имён, включённых в строку
     * с помощью Expr.exprQName()
     *
     * @param expr
     * @return
     */
    public static String exprReplaceAliasesForQNames(String expr, RenamingDict renames){
        return
                exprReplaceAliasesForQNames(
                        exprReplaceAliasesForQNames(
                                expr,
                                ExprConsts.QRNAME, renames),
                        ExprConsts.QNAME, renames);
    }

    /** Выполняет по переданному словарю замены алиасных частей квалифицированных имён,
     * включённых в строку с помощью Expr.exprQ[R]Name()
     *
     * @param expr
     * @return
     */
    protected static String exprReplaceAliasesForQNames(String expr, String tag, RenamingDict renames){
        if (renames == null)
            return expr;
        String tmplStart = ExprConsts.EXPR_BEGIN + tag + ExprConsts.DELIMITER;
        int posPrev = 0;
        StringBuilder sb = new StringBuilder();
        while (true) {
            int posBegin = expr.indexOf(tmplStart, posPrev);
            if (posBegin < 0) {
                sb.append(expr.substring(posPrev));
                break;
            }
            posBegin += tmplStart.length();
            int posEnd = expr.indexOf(ExprConsts.EXPR_END, posBegin);
            Preconditions.checkState(posEnd >= 0);
            QualifiedName qname = QualifiedName.stringToQualifiedName(expr.substring(posBegin, posEnd));
            qname.alias = renames.rename(qname.alias);
            sb.append(expr, posPrev, posBegin);
            sb.append(qname.qualifiedNameToString());
            posPrev = posEnd;
        }
        return sb.toString();
    }

    private static final String QUOTES = "'";
    private static final String UNCLOSED_QUOTE = "Незакрытая кавычка %s в выражении %s";
    /**
     * Извлекает из переданной строки с указанной позиции литерал
     *
     * @param str
     *            исходная строка
     * @param fromPos
     *            стартовая позиция, содержащая открывающую кавычку
     * @return литерал - строка, заключённая в кавычки
     */
    public static String getLiteral(String str, int fromPos) {
        int currentPos = fromPos;
        Preconditions.checkPositionIndex(currentPos, str.length() - 1, str);
        Preconditions.checkArgument(QUOTES.indexOf(str.charAt(currentPos)) >= 0);
        Character firstQuote = str.charAt(currentPos);
        String pairQuotes = firstQuote.toString() + firstQuote.toString();
        StringBuilder result = new StringBuilder();
        result.append(firstQuote.toString());
        currentPos++;
        boolean isSuccess = false;
        while (currentPos < str.length()) {
            if (StringUtils.mid(str, currentPos, pairQuotes.length()).equals(pairQuotes)) {
                currentPos += 2;
                result.append(pairQuotes);
            }
            else if (str.charAt(currentPos) == firstQuote.charValue()) {
                isSuccess = true;
                result.append(firstQuote);
                break;
            }
            else {
                result.append(str.charAt(currentPos));
                currentPos++;
            }
        }
        Preconditions.checkArgument(isSuccess, String.format(UNCLOSED_QUOTE, firstQuote, str));
        return result.toString();
    }

    public static int getPosAfterLiteral(String str, int fromPos) {
        int currentPos = fromPos;
        Preconditions.checkPositionIndex(currentPos, str.length() - 1, str);
        Preconditions.checkArgument(QUOTES.indexOf(str.charAt(currentPos)) >= 0);
        Character firstQuote = str.charAt(currentPos);
        String pairQuotes = firstQuote.toString() + firstQuote.toString();
        currentPos++;
        boolean isSuccess = false;
        while (currentPos < str.length()) {
            if (StringUtils.mid(str, currentPos, pairQuotes.length()).equals(pairQuotes)) {
                currentPos += 2;
            }
            else if (str.charAt(currentPos) == firstQuote.charValue()) {
                isSuccess = true;
                currentPos++;
                break;
            }
            else {
                currentPos++;
            }
        }
        Preconditions.checkArgument(isSuccess, String.format(UNCLOSED_QUOTE, firstQuote, str));
        return currentPos;
    }

    public static int getUnnamedRefCount(String str) throws ExpressionException {
        int refCount = 0;
        Preconditions.checkNotNull(str);
        int pos = 0;
        while (pos < str.length()) {
            switch (str.charAt(pos)) {
                case '\'':
                    pos = getPosAfterLiteral(str, pos);
                    pos++;
                    break;
                case Expression.CHAR_BEFORE_TOKEN: {
                    // считая количество безымянных ссылок на аргументы, подразумеваем,
                    // что именованых ссылок [уже] нет
                    if (!str.substring(pos, pos + Expression.UNNAMED_ARGUMENT_REF.length()).equals(Expression.UNNAMED_ARGUMENT_REF))
                        throw new ExpressionException(String.format(EXPECTED_UNNAMED_REFS, Preconditions.checkNotNull(str)));
                    refCount++;
                    pos += Expression.UNNAMED_ARGUMENT_REF.length();
                    break;
                }
                default:
                    pos++;
            }
        }
        return refCount;
    }

    public static String[] getParamRefs(String str) {
        List<String> result = new ArrayList<String>();
        Preconditions.checkNotNull(str);
        int pos = 0;
        while (pos < str.length()) {
            switch (str.charAt(pos)) {
                case '\'':
                    pos = getPosAfterLiteral(str, pos);
                    pos++;
                    break;
                case Expression.CHAR_BEFORE_PARAMETER: {
                    String paramName = getParamName(str, pos);
                    pos += paramName.length();
                    result.add(paramName.substring(1));
                    break;
                }
                default:
                    pos++;
            }
        }
        return result.toArray(new String[0]);
    }

    protected static boolean isLetter(char ch, boolean enableCyr) {
        return Character.isLetter(ch) ||
                (enableCyr && Character.UnicodeBlock.of(ch).equals(Character.UnicodeBlock.CYRILLIC));
    }

    /**
     * Извлекает из переданной строки с указанной позиции имя параметра
     *
     * @param str
     *            исходная строка
     * @param fromPos
     *            стартовая позиция, содержащая префикс параметра ':'
     * @return
     *            имя параметра с префиксом ':', начинающееся с буквы (латинской),
     *            содержащее буквы (латинские), цифры и символы '_', '$'
     */
	public static String getParamName(String str, int fromPos) {
        int currentPos = fromPos;
        Preconditions.checkPositionIndex(currentPos, str.length() - 1, str);
        Preconditions.checkArgument(str.charAt(currentPos) == Expression.CHAR_BEFORE_PARAMETER);
        currentPos ++;
        StringBuilder result = new StringBuilder();
        char ch;
        while (currentPos < str.length()) {
            ch = str.charAt(currentPos);
            if ((result.length() == 0 && isLetter(ch, false)) ||
                    (result.length() != 0 && (isLetter(ch, false) || Character.isDigit(ch) || ch == '$' || ch == '_'))
                    ) {
                result.append(ch);
                currentPos++;
            }
            else
                break;
        }
        Preconditions.checkArgument(result.length() != 0);
        result.insert(0, Expression.CHAR_BEFORE_PARAMETER);
        return result.toString();
    }

    /**
     * Извлекает из переданной строки с указанной позиции последовательность вида `?N1A$_MEБЪ23D`,
     * т.е. начинающуюся с буквы и содержащую буквы, цифры и '_'/'$'
     * или `??`
     *
     * @param str
     *            исходная строка
     * @param fromPos
     *            стартовая позиция, содержащая префикс '?'
     * @return
     *
     *
     */
    public static String getToken(String str, int fromPos) {
        int currentPos = fromPos;
        Preconditions.checkPositionIndex(currentPos, str.length() - 1, str);
        Preconditions.checkArgument(str.charAt(currentPos) == Expression.CHAR_BEFORE_TOKEN);
        if (str.substring(currentPos, currentPos + Expression.UNNAMED_ARGUMENT_REF.length()).equals(Expression.UNNAMED_ARGUMENT_REF))
            return Expression.UNNAMED_ARGUMENT_REF;

        currentPos++;
        StringBuilder result = new StringBuilder();
        char ch;
        while (currentPos < str.length()) {
            ch = str.charAt(currentPos);
            if ((result.length() == 0 && isLetter(ch, true)) ||
                    (result.length() != 0 && (isLetter(ch, true) || Character.isDigit(ch) || ch == '$' || ch == '_'))
                    ) {
                result.append(ch);
                currentPos++;
            }
            else
                break;
        }
        Preconditions.checkArgument(result.length() != 0);
        result.insert(0, Expression.CHAR_BEFORE_TOKEN);
        return result.toString();
    }
}
