package ru.sonarplus.kernel.sqlobject.common_utils;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.TreeMap;

/** Простой словарь переименований.
 * void add(String source, String target)
 *   добавляет переименование source->target.
 *   Попытка добавить переименование для source при уже имеющемся дл него переименовании приводит к ошибке.
 * boolean isRenamed(String source)
 *   проверка наличия переименования для source. Возвращает true при ранее успешном выполнении add
 * String rename(Stirng source)
 *   вернуть переименование, при его наличии, в противном случае вернуть source
 *
 */
public class RenamingDict {

    private Map<String, String> dict = null;
    private static final String ALREADY_RENAMED =
            "При попытке определить для элемента \"%s\" переименование в \"%s\" обнаружено переименование в \"%s\"";

    public RenamingDict() {

    }

    public void add(String source, String target) {
        String renaming = tryRename(source);
        Preconditions.checkNotNull(target);
        if (renaming != null) {
            Preconditions.checkState(
                    renaming.compareToIgnoreCase(target) == 0,
                    String.format(ALREADY_RENAMED, source, target, renaming));
            return;
        }
        if (dict == null)
            dict = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        dict.put(source, target);
    }

    protected String tryRename(String source) {
        Preconditions.checkNotNull(source);
        if (dict != null)
            return dict.get(source);
        return null;
    }

    public boolean isRenamed(String source) {
        return tryRename(source) != null;
    }

    public String rename(String source) {
        String renaming = tryRename(source);
        return renaming != null ? renaming : source;
    }
}
