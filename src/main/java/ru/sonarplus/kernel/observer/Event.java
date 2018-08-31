package ru.sonarplus.kernel.observer;

/**
 * ���������� � �������, ������ TComEvent � Delphi.
 */
public interface Event {

    Object getParameters();

    void passEvent() throws Exception;

    Observable getObservable();
    
    
    boolean isDone();
    
    void setDone(boolean done);
}