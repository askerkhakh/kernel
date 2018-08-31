package ru.sonarplus.kernel.observer;

/**
 * Простой интерфейс, позволяющий за счёт агрегации {@link ru.sonarplus.kernel.observer.impl.EventManager} в своём
 * объекте и реализации единственного метода, поддержать механизм рассылки событий в любом объекте.
 */
public interface ObservableContainer {

    Observable getObservable();

}