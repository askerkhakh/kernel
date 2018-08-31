package ru.sonarplus.kernel.observer;

/**
 * ������� ���������, ����������� �� ���� ��������� {@link ru.sonarplus.kernel.observer.impl.EventManager} � ����
 * ������� � ���������� ������������� ������, ���������� �������� �������� ������� � ����� �������.
 */
public interface ObservableContainer {

    Observable getObservable();

}