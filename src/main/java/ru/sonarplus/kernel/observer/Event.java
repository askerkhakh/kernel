package ru.sonarplus.kernel.observer;

/**
 * Информация о событии, аналог TComEvent в Delphi.
 */
public interface Event {

    Object getParameters();

    void passEvent() throws Exception;

    Observable getObservable();
    
    
    boolean isDone();
    
    void setDone(boolean done);
}