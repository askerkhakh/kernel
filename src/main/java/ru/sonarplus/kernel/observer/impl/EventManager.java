package ru.sonarplus.kernel.observer.impl;

import ru.sonarplus.kernel.observer.Event;
import ru.sonarplus.kernel.observer.Observable;
import ru.sonarplus.kernel.observer.Observer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;

/**
 * Реализация механизма рассылки событий
 */
public class EventManager implements Observable {

    private static class EventImpl implements Event{

        private final Object parameters;
    
        private boolean done;

        boolean first;

        EventInfo eventInfo;

        @Nullable
        Observer defaultHandler;
        
        int currentIndex;
    
        EventImpl(Object parameters) {
            this.parameters = parameters;
        }
    
        @Override
        public Object getParameters() {
            return parameters;
        }

        @Override
        public void passEvent() throws Exception {
            eventInfo.passEvent(this);
        }

        @Override
        public Observable getObservable() {
            return eventInfo.getOwnerEventList().getOwnerObservable();
        }
        
        @Override
        public boolean isDone() {
            return done;
        }
    
        @Override
        public void setDone(boolean done) {
            this.done = done;
        }
        
    }


    private static class EventList {

        private final Map<Class<?>, EventInfo> eventMap = new HashMap<>();

        private final Observable ownerObservable;

        EventList(Observable ownerObservable) {
            this.ownerObservable = ownerObservable;
        }

        Observable getOwnerObservable() {
            return ownerObservable;
        }

        void addEventHandler(Class<?> eventParametersClass, Observer handler) {
            EventInfo eventInfo = eventMap.get(eventParametersClass);
            if (eventInfo != null) {
                eventInfo.lock();
                try {
                    eventInfo.addHandler(handler);
                }
                finally {
                    eventInfo.unlock();
                }
            }
            else {
                eventInfo = new EventInfo(this);
                eventMap.put(eventParametersClass, eventInfo);

                eventInfo.lock();
                try {
                    // sendEventListChange();
                    eventInfo.addHandler(handler);
                }
                finally {
                    eventInfo.unlock();
                }
            }
        }

        void deleteEventHandler(Class<?> eventParametersClass, Observer handler) {
            EventInfo eventInfo = eventMap.get(eventParametersClass);
            if (eventInfo == null) {
                return;
            }
            eventInfo.lock();
            try {
                eventInfo.removeHandler(handler);
            }
            finally {
                eventInfo.unlock();
            }
            
        }

        EventInfo getEventInfo(Class<?> eventParametersClass) {
            return eventMap.get(eventParametersClass);
        }

    }


    private static class EventInfo {

        private List<Observer> handlers = new ArrayList<>();

        private final EventList ownerEventList;
        
        private int lockCount = 0;
        
        private boolean needToClear = false;

        EventInfo(EventList ownerEventList) {
            this.ownerEventList = ownerEventList;
        }

        EventList getOwnerEventList() {
            return ownerEventList;
        }

        void lock() {
            lockCount++;
        }

        void unlock() {
            lockCount--;
            verify(lockCount >= 0);
            if ((lockCount == 0) && needToClear) {
                clear();
            }
        }

        private void clear() {
            needToClear = false;

            handlers.removeIf(Objects::isNull);
        }

        void sendEvent(EventImpl event) throws Exception {
            checkNotNull(event);
            if (event.first) {
                event.first = false;
                event.currentIndex = handlers.size() - 1;
            }
            else {
                verify(event.eventInfo == this);
                event.currentIndex--;
            }
            while (!event.isDone() && (event.currentIndex >= 0)) {
                Observer currentHandler = handlers.get(event.currentIndex);
                if (currentHandler != null) {
                    verify(currentHandler != event.defaultHandler);
                    currentHandler.handleEvent(event);
                }
                event.currentIndex--;
            }

            if (!event.isDone() && event.defaultHandler != null) {
                Observer defaultHandler = event.defaultHandler;
                event.defaultHandler = null;
                defaultHandler.handleEvent(event);
            }
        }

        void passEvent(EventImpl event) throws Exception {
            event.first = false;
            sendEvent(event);
        }

        void addHandler(Observer handler) {
            if (!handlers.contains(handler)) {
                handlers.add(handler);
            }
        }

        void removeHandler(Observer handler) {
            int index = handlers.indexOf(handler);
            if (index < 0) {
                return;
            }
            handlers.set(index, null);
            needToClear = true;
        }

    }

    
    private EventList eventList;

    private EventList getEventList() {
        if (eventList == null) {
            eventList = new EventList(this);
        }
        return eventList;
    }

    @Nullable
    private EventInfo getEventInfo(Class<?> eventParametersClass) {
        if (eventList == null) {
            return null;
        }
        return eventList.getEventInfo(eventParametersClass);
    }

    public void installEventHandler(Class<?> eventParametersClass, Observer handler) {
        getEventList().addEventHandler(eventParametersClass, handler);
    }

    public void removeEventHandler(Class<?> eventParametersClass, Observer handler) {
        getEventList().deleteEventHandler(eventParametersClass, handler);
    }

    public void SendEvent(Object parameters, @Nullable Observer defaultHandler) throws Exception {
        EventInfo eventInfo = getEventInfo(parameters.getClass());
        if ((eventInfo == null) && (defaultHandler == null)) {
            return;
        }
        EventImpl event = new EventImpl(parameters);
        if (eventInfo == null) {
            defaultHandler.handleEvent(event);
        }
        else {
            event.first = true;
            event.eventInfo = eventInfo;
            event.defaultHandler = defaultHandler;
            eventInfo.lock();
            try {
                eventInfo.sendEvent(event);
            }
            finally {
                eventInfo.unlock();
            }
        }
    }
    
}