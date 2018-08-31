package ru.sonarplus.kernel;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import ru.sonarplus.kernel.data_change_service.DataChangeService;

/**
 * KernelInitialization
 */
@Named
@Singleton
public class KernelInitialization {

    private final DataChangeService dataChangeService;

    @Inject
    public KernelInitialization(DataChangeService dataChangeService) {
        this.dataChangeService = dataChangeService;
    }

    public void initialize() {
        dataChangeService.registerGeneralTrigger(KernelTrigger::trigger);
    }
}