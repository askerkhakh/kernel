package ru.sonarplus.kernel.observer;

/**
 * ���������� �������, �������� ����������� ��� ������.
 */
public interface Observer {

    void handleEvent(Event event) throws Exception;
}