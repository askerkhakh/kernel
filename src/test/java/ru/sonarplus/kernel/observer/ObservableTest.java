package ru.sonarplus.kernel.observer;

import org.junit.Before;
import org.junit.Test;
import ru.sonarplus.kernel.observer.impl.EventManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * ObservableTest
 */
public class ObservableTest {

    private static class TestEventParameters {

        int intValue;
        String stringValue;
        List<String> log;
    }

    private static final int INT_TEST_VALUE = 100500;
    private static final String STRING_TEST_VALUE = "SomeTestString";

    private Observable observable; 
    private TestEventParameters parameters;

    private static void VerifyParameters(TestEventParameters parameters) {
        assertThat(parameters.intValue, is(INT_TEST_VALUE));
        assertThat(parameters.stringValue, is(STRING_TEST_VALUE));
    }

    @Before
    public void initialize() {
        observable = new EventManager();
        parameters = new TestEventParameters();
        parameters.intValue = INT_TEST_VALUE;
        parameters.stringValue = STRING_TEST_VALUE;
        parameters.log = new ArrayList<>();
    }

    @Test
    public void noHandlers() throws Exception {
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.emptyList()));
    }

    private void methodHandler(Event event) {
        TestEventParameters p = (TestEventParameters) event.getParameters();
        VerifyParameters(p);
        p.log.add("method_handler");
    }
    @Test
    public void methodAsHandler() throws Exception {
        observable.installEventHandler(parameters.getClass(), this::methodHandler);
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("method_handler")));
    }

    private static void staticMethodHandler(Event event) {
        TestEventParameters p = (TestEventParameters) event.getParameters();
        VerifyParameters(p);
        p.log.add("static_method_handler");
    }
    @Test
    public void staticMethodAsHandler() throws Exception {
        observable.installEventHandler(parameters.getClass(), ObservableTest::staticMethodHandler);
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("static_method_handler")));
    }

    @Test
    public void defaultHandler() throws Exception {
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Collections.singletonList("default")));
    }

    @Test
    public void handlerAndDefaultHandler() throws Exception {
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("handler");
        });
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Arrays.asList("handler", "default")));
    }

    @Test
    public void passEventHandler() throws Exception {
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("before_passEvent");
            e.passEvent();
            p.log.add("after_passEvent");
        });
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Arrays.asList("before_passEvent", "default", "after_passEvent")));
    }

    @Test
    public void eventDone() throws Exception {
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("handler");
            e.setDone(true);
        });
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Collections.singletonList("handler")));
    }

    @Test
    public void passEventTwoHandlers() throws Exception {
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("<first>");
            e.passEvent();
            p.log.add("</first>>");
        });
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("<second>");
            e.passEvent();
            p.log.add("</second>");
        });
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Arrays.asList("<second>", "<first>", "default", "</first>>", "</second>")));
    }

    @Test
    public void installHandlerDuringSendEvent() throws Exception {
        Observer second_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("<second>");
            e.passEvent();
            p.log.add("</second>");
        };
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("<first>");
            e.getObservable().installEventHandler(p.getClass(), second_handler);
            e.passEvent();
            p.log.add("</first>>");
        });
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Arrays.asList("<first>", "default", "</first>>")));

        parameters.log.clear();
        observable.SendEvent(parameters, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("default");
        });
        assertThat(parameters.log, is(Arrays.asList("<second>", "<first>", "default", "</first>>", "</second>")));
    }

    @Test
    public void removeWorkedHandlerDuringSendEvent() throws Exception {
        Observer second_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("second");
        };
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("first");
            e.getObservable().removeEventHandler(p.getClass(), second_handler);
        });
        observable.installEventHandler(TestEventParameters.class, second_handler);
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Arrays.asList("second", "first")));

        parameters.log.clear();
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("first")));
    }

    @Test
    public void removeNextHandlerDuringSendEvent() throws Exception {
        Observer first_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("first");
        };
        observable.installEventHandler(TestEventParameters.class, first_handler);
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("second");
            e.getObservable().removeEventHandler(p.getClass(), first_handler);
        });
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("second")));
    }

    @Test
    public void removeSomeNextHandlerDuringSendEvent() throws Exception {
        Observer second_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("second");
        };
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("first");
        });
        observable.installEventHandler(TestEventParameters.class, second_handler);
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("third");
            e.getObservable().removeEventHandler(p.getClass(), second_handler);
        });
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Arrays.asList("third", "first")));
    }

    @Test
    public void removeAllNextHandlersDuringSendEvent() throws Exception {
        Observer second_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("second");
        };
        Observer first_handler = (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("first");
        };        
        observable.installEventHandler(TestEventParameters.class, first_handler);
        observable.installEventHandler(TestEventParameters.class, second_handler);
        observable.installEventHandler(TestEventParameters.class, (Event e) -> {
            TestEventParameters p = (TestEventParameters) e.getParameters();
            VerifyParameters(p);
            p.log.add("third");
            e.getObservable().removeEventHandler(p.getClass(), second_handler);
            e.getObservable().removeEventHandler(p.getClass(), first_handler);
        });
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("third")));
    }

    private final Observer handlerThatRemovesHimself = this::methodThatRemovesHimself;

    private void methodThatRemovesHimself(Event event) {
        TestEventParameters p = (TestEventParameters) event.getParameters();
        VerifyParameters(p);
        p.log.add("first");
        event.getObservable().removeEventHandler(p.getClass(), handlerThatRemovesHimself);
    }
    @Test
    public void removeCurrentHandlerDuringSendEvent() throws Exception {
        observable.installEventHandler(TestEventParameters.class, handlerThatRemovesHimself);
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.singletonList("first")));

        parameters.log.clear();
        observable.SendEvent(parameters);
        assertThat(parameters.log, is(Collections.emptyList()));
    }

}