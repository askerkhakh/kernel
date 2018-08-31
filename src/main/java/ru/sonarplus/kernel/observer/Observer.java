package ru.sonarplus.kernel.observer;

/**
 * Обработчик события, зачастую реализуется как лямбда.
 */
public interface Observer {

    void handleEvent(Event event) throws Exception;
}