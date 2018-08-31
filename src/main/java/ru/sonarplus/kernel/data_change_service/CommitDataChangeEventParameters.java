package ru.sonarplus.kernel.data_change_service;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.recordset.TableRecord;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * CommitDataChangeEventParameters
 */
public class CommitDataChangeEventParameters {

    // in:
    public ClientSession session;
    public DataChangeOperation operation;
    public TableRecord record;
    @Nullable
    public Map<String, String> options; // опции выполнения триггеров, может содержать доп. информацию, влияющую на работу того или иного триггера
    // out:
    @Nullable
    public Long calculatedPrimaryKey; // значение рассчитанного первичного ключа, если нет первичного ключа или он не рассчитывается, то null
    public boolean succsess; // признак успешности для операций редактирования и удаления
}