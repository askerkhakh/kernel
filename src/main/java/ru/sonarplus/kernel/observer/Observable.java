package ru.sonarplus.kernel.observer;

import javax.annotation.Nullable;

/**
 * ������, �������������� ���� ��������� ����� �������������� ����������� � ��������� � ���� �������.
 */
public interface Observable {

    void installEventHandler(Class<?> eventParametersClass, Observer handler);

    void removeEventHandler(Class<?> eventParametersClass, Observer handler);


    void SendEvent(Object parameters, @Nullable Observer defaultHandler) throws Exception;

    default void SendEvent(Object parameters) throws Exception {
        SendEvent(parameters, null);
    }

}