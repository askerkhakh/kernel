package ru.sonarplus.kernel;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * KernelConfig
 */
@Configuration
@ComponentScan(basePackageClasses = PackageMarker.class)
public class KernelConfig {}