// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.lineage.dataworks;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.lineage.LineageContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.lineage.LineageInfo.DirectLineageType;
import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import org.apache.doris.extension.spi.PluginContext;

import com.google.common.base.Strings;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.action.Action;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.appender.rolling.action.Duration;
import org.apache.logging.log4j.core.appender.rolling.action.IfAccumulatedFileSize;
import org.apache.logging.log4j.core.appender.rolling.action.IfFileName;
import org.apache.logging.log4j.core.appender.rolling.action.IfLastModified;
import org.apache.logging.log4j.core.appender.rolling.action.PathCondition;
import org.apache.logging.log4j.core.appender.rolling.action.PathSortByModificationTime;
import org.apache.logging.log4j.core.appender.rolling.action.PathSorter;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.appender.AbstractFileAppender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Dataworks lineage plugin implementation.
 */
public class DataworksLineagePlugin implements LineagePlugin {

    private static final Logger LOG = LogManager.getLogger(DataworksLineagePlugin.class);
    private static final String PLUGIN_NAME = "dataworks";
    private static final String LINEAGE_LOGGER_NAME = "lineage.dataworks";
    private static final String LINEAGE_APPENDER_NAME = "LineageDataworksFile";
    private static final String LINEAGE_LOG_FILE = "lineage_dataworks.log";
    private static final String ACTION_TYPE = "Lineage";
    private static final String EMPTY_STRING = "";
    private static final String HASH_SEPARATOR = "|";
    private static final String SCOPE_TABLE = "table";
    private static final String SCOPE_COLUMN = "column";
    private static final String LINEAGE_LOG_PATTERN = "%m%n";
    private static final String ENABLED_SCOPE_KEY = "lineage_dataworks_enabled_scope";
    private static final String LOG_PREFIX = "[dataworks-lineage] ";
    private static final String STATE_SUCCESS = "SUCCESS";
    private static final String STATE_FAILED = "FAILED";
    private static final String STATE_OK = "OK";
    private static final String STATE_EOF = "EOF";
    private static final String STATE_NOOP = "NOOP";
    private static final int ROLLING_TIME_POLICY_INTERVAL = 1;
    private static final int DELETE_ACTION_MAX_DEPTH = 1;
    private static final int SINGLE_SOURCE_TABLE_COUNT = 1;

    private final Gson gson;
    private final Object logConfigLock = new Object();
    private volatile boolean logConfigured = false;
    private DataworksLogConfig logConfig = new DataworksLogConfig();
    private volatile String enabledScope = EMPTY_STRING;

    public DataworksLineagePlugin() {
        gson = new GsonBuilder().disableHtmlEscaping().create();
    }

    @Override
    public void initialize(PluginContext context) {
        Map<String, String> properties = context == null ? Collections.emptyMap() : context.getProperties();
        synchronized (logConfigLock) {
            if (logConfigured) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(LOG_PREFIX + "Dataworks lineage logger already configured, skip init. properties={}",
                            properties.keySet());
                }
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Init Dataworks lineage plugin. properties={}",
                        properties.keySet());
            }
            loadLogConfig(properties);
            configureLineageLogger();
            logConfigured = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Dataworks lineage plugin init completed. loggerConfigured={}, loggerName={}, appenderName={}",
                        logConfigured, LINEAGE_LOGGER_NAME, LINEAGE_APPENDER_NAME);
            }
        }
    }

    @Override
    public void close() {
        synchronized (logConfigLock) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Close Dataworks lineage plugin logger. loggerName={}, appenderName={}",
                        LINEAGE_LOGGER_NAME, LINEAGE_APPENDER_NAME);
            }
            removeLineageLogger();
            logConfigured = false;
        }
    }

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean eventFilter() {
        boolean activated = isPluginActivated();
        ScopeFlags scopeFlags = parseScopes(enabledScope);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX
                            + "Dataworks lineage eventFilter check. activated={}, enabledScope='{}', emitTable={}, emitColumn={}",
                    activated, Strings.nullToEmpty(enabledScope), scopeFlags.emitTable, scopeFlags.emitColumn);
        }
        return activated && scopeFlags.enabled();
    }

    @Override
    public boolean exec(LineageInfo lineageInfo) {
        if (lineageInfo == null || lineageInfo.getContext() == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Skip Dataworks lineage exec because lineageInfo or context is null");
            }
            return false;
        }
        ScopeFlags scopeFlags = parseScopes(enabledScope);
        if (!scopeFlags.enabled()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Skip Dataworks lineage exec because enabledScope is disabled. enabledScope='{}'",
                        Strings.nullToEmpty(enabledScope));
            }
            return true;
        }
        if (lineageInfo.getTargetTable() == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Skip Dataworks lineage exec because targetTable is null. queryId={}",
                        lineageInfo.getContext().getQueryId());
            }
            return true;
        }
        DataworkLineageInfo lineageInfoDetail = buildLineageDetailInfo(lineageInfo, scopeFlags);
        if (lineageInfoDetail == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Skip Dataworks lineage exec because lineage detail is null. queryId={}",
                        lineageInfo.getContext().getQueryId());
            }
            return true;
        }
        ensureLineageLoggerBinding(lineageInfoDetail.queryId);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Emit Dataworks lineage log. queryId={}, action={}, columnLineageSize={}",
                    lineageInfoDetail.queryId, lineageInfoDetail.action,
                    lineageInfoDetail.columnLineages == null ? 0 : lineageInfoDetail.columnLineages.size());
            logRuntimeLoggerBinding("before-write", lineageInfoDetail.queryId);
        }
        getLineageLogger().info(gson.toJson(lineageInfoDetail));
        if (LOG.isDebugEnabled()) {
            logRuntimeLoggerBinding("after-write", lineageInfoDetail.queryId);
        }
        return true;
    }

    private void loadLogConfig(Map<String, String> pluginProperties) {
        Properties properties = new Properties();
        String pluginPathStr = pluginProperties == null ? null : pluginProperties.get("plugin.path");
        if (!Strings.isNullOrEmpty(pluginPathStr)) {
            Path pluginPath = FileSystems.getDefault().getPath(pluginPathStr);
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX
                                + "Try loading dataworks plugin.conf. pluginPath='{}', absolutePath='{}', exists={}, readable={}",
                        pluginPathStr, pluginPath.toAbsolutePath(),
                        Files.exists(pluginPath), Files.isReadable(pluginPath));
            }
            if (Files.exists(pluginPath)) {
                Path confFile = pluginPath.resolve("plugin.conf");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(LOG_PREFIX + "Inspect plugin conf path='{}', exists={}, readable={}", confFile,
                            Files.exists(confFile), Files.isReadable(confFile));
                }
                if (Files.exists(confFile)) {
                    try (InputStream stream = Files.newInputStream(confFile)) {
                        properties.load(stream);
                    } catch (IOException e) {
                        LOG.warn(LOG_PREFIX + "failed to load plugin conf file: {}", confFile, e);
                    }
                } else {
                    LOG.warn(LOG_PREFIX + "plugin conf file does not exist: {}", confFile);
                }
            } else {
                LOG.warn(LOG_PREFIX + "plugin path does not exist: {}", pluginPath);
            }
        } else {
            LOG.warn(LOG_PREFIX + "plugin path is empty when loading plugin.conf. pluginProperties={}",
                    pluginProperties == null ? "" : pluginProperties.keySet());
        }
        if (pluginProperties != null) {
            for (Map.Entry<String, String> entry : pluginProperties.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    properties.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
        logConfig = DataworksLogConfig.from(properties);
        enabledScope = getTrimmedProperty(properties, ENABLED_SCOPE_KEY, EMPTY_STRING);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX
                            + "Dataworks lineage config loaded. enabledScope='{}', logDir='{}', rollNum={}, rollMaxSizeMb={},"
                            + " rollInterval='{}', deleteAge='{}', accumulatedFileSizeGb={}, configKeys={}",
                    Strings.nullToEmpty(enabledScope), Strings.nullToEmpty(logConfig.logDir), logConfig.rollNum,
                    logConfig.rollMaxSizeMb, Strings.nullToEmpty(logConfig.rollInterval),
                    Strings.nullToEmpty(logConfig.deleteAge), logConfig.accumulatedFileSizeGb,
                    properties.stringPropertyNames());
        }
    }

    private String getTrimmedProperty(Properties properties, String key, String defaultValue) {
        if (properties == null) {
            return defaultValue;
        }
        String value = properties.getProperty(key);
        return Strings.isNullOrEmpty(value) ? defaultValue : value.trim();
    }

    private void configureLineageLogger() {
        LoggerContext context = resolveLoggerContext("configure");
        Configuration configuration = context.getConfiguration();
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX
                            + "Configure Dataworks lineage logger start. loggerName={}, appenderName={}, resolvedLogDir='{}',"
                            + " logDirSource={}, contextName={}, contextIdentity={}, configAppenders={}",
                    LINEAGE_LOGGER_NAME, LINEAGE_APPENDER_NAME, resolveLogDir(),
                    resolveLogDirSource(), context.getName(), System.identityHashCode(context),
                    configuration.getAppenders().keySet());
        }
        Appender appender = configuration.getAppender(LINEAGE_APPENDER_NAME);
        if (appender == null) {
            appender = buildAppender(configuration);
            if (appender == null) {
                LOG.warn(LOG_PREFIX + "Skip Dataworks lineage logger configuration because appender build returned null. "
                                + "loggerName={}, appenderName={}, resolvedLogDir='{}'",
                        LINEAGE_LOGGER_NAME, LINEAGE_APPENDER_NAME, resolveLogDir());
                return;
            }
            appender.start();
            configuration.addAppender(appender);
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Dataworks lineage appender created. appenderName={}, appenderClass={}, file={}",
                        appender.getName(), appender.getClass().getSimpleName(), getAppenderFile(appender));
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Reuse existing Dataworks lineage appender. appenderName={}, appenderClass={}, file={}",
                    appender.getName(), appender.getClass().getSimpleName(), getAppenderFile(appender));
        }

        LoggerConfig loggerConfig = configuration.getLoggerConfig(LINEAGE_LOGGER_NAME);
        if (loggerConfig.getName().equals(LINEAGE_LOGGER_NAME)) {
            if (!loggerConfig.getAppenders().containsKey(LINEAGE_APPENDER_NAME)) {
                loggerConfig.addAppender(appender, Level.INFO, null);
            }
        } else {
            LoggerConfig newConfig = new LoggerConfig(LINEAGE_LOGGER_NAME, Level.INFO, false);
            newConfig.addAppender(appender, Level.INFO, null);
            configuration.addLogger(LINEAGE_LOGGER_NAME, newConfig);
        }
        context.updateLoggers();
        LoggerConfig effectiveLoggerConfig = configuration.getLoggerConfig(LINEAGE_LOGGER_NAME);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Configure Dataworks lineage logger end. effectiveLogger='{}', additive={}, appenders={},"
                            + " dedicatedAppenderExists={}, dedicatedAppenderFile={}",
                    effectiveLoggerConfig.getName(), effectiveLoggerConfig.isAdditive(),
                    effectiveLoggerConfig.getAppenders().keySet(),
                    configuration.getAppender(LINEAGE_APPENDER_NAME) != null,
                    getAppenderFile(configuration.getAppender(LINEAGE_APPENDER_NAME)));
        }
    }

    private void removeLineageLogger() {
        LoggerContext context = resolveLoggerContext("remove");
        Configuration configuration = context.getConfiguration();
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Remove Dataworks lineage logger start. loggerName={}, appenderName={}, contextName={},"
                            + " contextIdentity={}, configAppenders={}",
                    LINEAGE_LOGGER_NAME, LINEAGE_APPENDER_NAME,
                    context.getName(), System.identityHashCode(context),
                    configuration.getAppenders().keySet());
        }
        LoggerConfig loggerConfig = configuration.getLoggerConfig(LINEAGE_LOGGER_NAME);
        if (loggerConfig.getAppenders().containsKey(LINEAGE_APPENDER_NAME)) {
            loggerConfig.removeAppender(LINEAGE_APPENDER_NAME);
        }
        if (loggerConfig.getName().equals(LINEAGE_LOGGER_NAME) && loggerConfig.getAppenders().isEmpty()) {
            configuration.removeLogger(LINEAGE_LOGGER_NAME);
        }
        Appender appender = configuration.getAppender(LINEAGE_APPENDER_NAME);
        if (appender != null) {
            appender.stop();
            configuration.getAppenders().remove(LINEAGE_APPENDER_NAME);
        }
        context.updateLoggers();
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Remove Dataworks lineage logger end. loggerName={}, removedAppender={}, remainingAppenders={}",
                    LINEAGE_LOGGER_NAME, appender != null, configuration.getAppenders().keySet());
        }
    }

    private Appender buildAppender(Configuration configuration) {
        String logDir = resolveLogDir();
        if (Strings.isNullOrEmpty(logDir)) {
            LOG.warn(LOG_PREFIX + "dataworks lineage log dir is empty, skip appender initialization");
            return null;
        }
        String fileName = logDir + "/" + LINEAGE_LOG_FILE;
        String filePattern = logDir + "/" + LINEAGE_LOG_FILE + "." + logConfig.resolveRollPattern() + "-%i";
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Build Dataworks lineage appender with path settings. logDir='{}', logDirSource={}, fileName='{}',"
                            + " filePattern='{}', rollNum={}, rollMaxSizeMb={}, rollInterval='{}', deleteAge='{}',"
                            + " accumulatedFileSizeGb={}, logRolloverStrategy='{}'",
                    logDir, resolveLogDirSource(), fileName, filePattern, logConfig.rollNum,
                    logConfig.rollMaxSizeMb, logConfig.rollInterval, logConfig.deleteAge,
                    logConfig.accumulatedFileSizeGb, Strings.nullToEmpty(Config.log_rollover_strategy));
        }
        logAppenderFsState(logDir, fileName);
        try {
            Files.createDirectories(FileSystems.getDefault().getPath(logDir));
        } catch (IOException e) {
            LOG.warn(LOG_PREFIX + "failed to create lineage log dir: {}", logDir, e);
        }
        logAppenderFsState(logDir, fileName);
        PatternLayout layout = PatternLayout.newBuilder()
                .withCharset(StandardCharsets.UTF_8)
                .withPattern(LINEAGE_LOG_PATTERN)
                .build();
        TriggeringPolicy policy = CompositeTriggeringPolicy.createPolicy(
                TimeBasedTriggeringPolicy.newBuilder()
                        .withInterval(ROLLING_TIME_POLICY_INTERVAL)
                        .withModulate(true)
                        .build(),
                SizeBasedTriggeringPolicy.createPolicy(logConfig.rollMaxSizeMb + "MB"));
        RolloverStrategy strategy = buildRolloverStrategy(configuration, logConfig, logDir);

        Appender appender = RollingFileAppender.newBuilder()
                .withName(LINEAGE_APPENDER_NAME)
                .withFileName(fileName)
                .withFilePattern(filePattern)
                .withPolicy(policy)
                .withStrategy(strategy)
                .withAppend(true)
                .withLayout(layout)
                .withConfiguration(configuration)
                .build();
        if (appender == null) {
            LOG.warn(LOG_PREFIX + "RollingFileAppender builder returned null. appenderName={}, fileName='{}', filePattern='{}'",
                    LINEAGE_APPENDER_NAME, fileName, filePattern);
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "RollingFileAppender build success. appenderName={}, appenderClass={}, file={}",
                    appender.getName(), appender.getClass().getSimpleName(), getAppenderFile(appender));
        }
        return appender;
    }

    private String resolveLogDir() {
        String logDir = logConfig.logDir;
        if (Strings.isNullOrEmpty(logDir)) {
            logDir = System.getenv("LOG_DIR");
        }
        if (Strings.isNullOrEmpty(logDir)) {
            logDir = Strings.nullToEmpty(Config.sys_log_dir);
        }
        return logDir;
    }

    private String resolveLogDirSource() {
        if (!Strings.isNullOrEmpty(logConfig.logDir)) {
            return "plugin.conf";
        }
        if (!Strings.isNullOrEmpty(System.getenv("LOG_DIR"))) {
            return "env.LOG_DIR";
        }
        if (!Strings.isNullOrEmpty(Config.sys_log_dir)) {
            return "Config.sys_log_dir";
        }
        return "empty";
    }

    private String getAppenderFile(Appender appender) {
        if (appender instanceof AbstractFileAppender) {
            return ((AbstractFileAppender<?>) appender).getFileName();
        }
        return "";
    }

    private void logAppenderFsState(String logDir, String fileName) {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        Path logDirPath = FileSystems.getDefault().getPath(logDir);
        Path filePath = FileSystems.getDefault().getPath(fileName);
        Path parentPath = filePath.getParent();
        LOG.debug(LOG_PREFIX + "Dataworks lineage appender fs state. logDirPath='{}', exists={}, isDirectory={}, isWritable={},"
                        + " filePath='{}', fileExists={}, fileWritable={}, parentPath='{}', parentExists={},"
                        + " parentWritable={}",
                logDirPath, Files.exists(logDirPath), Files.isDirectory(logDirPath), Files.isWritable(logDirPath),
                filePath, Files.exists(filePath), Files.isWritable(filePath), parentPath,
                parentPath != null && Files.exists(parentPath),
                parentPath != null && Files.isWritable(parentPath));
    }

    private void logRuntimeLoggerBinding(String stage, String queryId) {
        LoggerContext context = resolveLoggerContext("runtime-" + stage);
        Configuration configuration = context.getConfiguration();
        LoggerConfig effectiveLoggerConfig = configuration.getLoggerConfig(LINEAGE_LOGGER_NAME);
        Appender dedicatedAppender = configuration.getAppender(LINEAGE_APPENDER_NAME);
        LOG.debug(LOG_PREFIX + "Dataworks lineage runtime logger binding. stage={}, queryId={}, contextName={}, contextIdentity={},"
                        + " effectiveLogger='{}', additive={}, effectiveAppenders={}, dedicatedAppenderExists={},"
                        + " dedicatedAppenderClass={}, dedicatedAppenderFile={}",
                stage, Strings.nullToEmpty(queryId), context.getName(), System.identityHashCode(context),
                effectiveLoggerConfig.getName(), effectiveLoggerConfig.isAdditive(),
                effectiveLoggerConfig.getAppenders().keySet(), dedicatedAppender != null,
                dedicatedAppender == null ? "" : dedicatedAppender.getClass().getSimpleName(),
                getAppenderFile(dedicatedAppender));
    }

    private void ensureLineageLoggerBinding(String queryId) {
        LoggerContext context = resolveLoggerContext("ensure-runtime-binding");
        if (hasDedicatedAppenderBinding(context)) {
            return;
        }
        synchronized (logConfigLock) {
            LoggerContext syncContext = resolveLoggerContext("ensure-runtime-binding-synchronized");
            if (hasDedicatedAppenderBinding(syncContext)) {
                return;
            }
            LOG.warn(LOG_PREFIX + "Detected missing runtime logger binding, try to reconfigure. queryId={}, "
                            + "contextName={}, contextIdentity={}",
                    Strings.nullToEmpty(queryId), syncContext.getName(), System.identityHashCode(syncContext));
            configureLineageLogger();
            LoggerContext refreshedContext = resolveLoggerContext("ensure-runtime-binding-after-reconfigure");
            if (!hasDedicatedAppenderBinding(refreshedContext)) {
                LOG.warn(LOG_PREFIX + "Runtime logger binding is still missing after reconfigure. queryId={}, "
                                + "contextName={}, contextIdentity={}",
                        Strings.nullToEmpty(queryId), refreshedContext.getName(),
                        System.identityHashCode(refreshedContext));
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Runtime logger binding recovered by reconfigure. queryId={}, contextName={},"
                                + " contextIdentity={}",
                        Strings.nullToEmpty(queryId), refreshedContext.getName(),
                        System.identityHashCode(refreshedContext));
            }
        }
    }

    private boolean hasDedicatedAppenderBinding(LoggerContext context) {
        if (context == null) {
            return false;
        }
        Configuration configuration = context.getConfiguration();
        if (configuration == null) {
            return false;
        }
        Appender dedicatedAppender = configuration.getAppender(LINEAGE_APPENDER_NAME);
        if (dedicatedAppender == null) {
            return false;
        }
        LoggerConfig effectiveLoggerConfig = configuration.getLoggerConfig(LINEAGE_LOGGER_NAME);
        return LINEAGE_LOGGER_NAME.equals(effectiveLoggerConfig.getName())
                && effectiveLoggerConfig.getAppenders().containsKey(LINEAGE_APPENDER_NAME);
    }

    private Logger getLineageLogger() {
        return LogManager.getLogger(LINEAGE_LOGGER_NAME);
    }

    private LoggerContext resolveLoggerContext(String phase) {
        Logger lineageLogger = getLineageLogger();
        if (lineageLogger instanceof org.apache.logging.log4j.core.Logger) {
            org.apache.logging.log4j.core.Logger coreLogger =
                    (org.apache.logging.log4j.core.Logger) lineageLogger;
            LoggerContext context = coreLogger.getContext();
            if (context != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(LOG_PREFIX
                                    + "Resolve logger context from lineage logger. phase={}, contextName={}, contextIdentity={}, loggerClass={}",
                            phase, context.getName(), System.identityHashCode(context),
                            lineageLogger.getClass().getName());
                }
                return context;
            }
        }
        LoggerContext fallback = (LoggerContext) LogManager.getContext(false);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX
                            + "Resolve logger context fallback to LogManager.getContext(false). phase={}, contextName={},"
                            + " contextIdentity={}",
                    phase, fallback.getName(), System.identityHashCode(fallback));
        }
        return fallback;
    }

    private RolloverStrategy buildRolloverStrategy(Configuration configuration, DataworksLogConfig conf, String logDir) {
        Action deleteAction = buildDeleteAction(configuration, conf, logDir);
        Action[] customActions = deleteAction == null ? null : new Action[] {deleteAction};
        return DefaultRolloverStrategy.newBuilder()
                .withMax(String.valueOf(conf.rollNum))
                .withFileIndex("max")
                .withCustomActions(customActions)
                .withConfig(configuration)
                .build();
    }

    private Action buildDeleteAction(Configuration configuration, DataworksLogConfig conf, String logDir) {
        PathCondition deleteCondition = buildDeleteCondition(conf);
        if (deleteCondition == null) {
            return null;
        }
        PathSorter sorter = PathSortByModificationTime.createSorter(true);
        return DeleteAction.createDeleteAction(logDir, false, DELETE_ACTION_MAX_DEPTH, false, sorter,
                new PathCondition[] {deleteCondition}, null, configuration);
    }

    private PathCondition buildDeleteCondition(DataworksLogConfig conf) {
        String strategy = Strings.nullToEmpty(Config.log_rollover_strategy);
        PathCondition nestedCondition;
        if ("size".equalsIgnoreCase(strategy)) {
            nestedCondition = IfAccumulatedFileSize.createFileSizeCondition(conf.accumulatedFileSizeGb + "GB");
        } else {
            Duration duration;
            try {
                duration = Duration.parse(conf.deleteAge);
            } catch (Exception e) {
                LOG.warn(LOG_PREFIX + "invalid delete age config for dataworks lineage: {}", conf.deleteAge, e);
                return null;
            }
            nestedCondition = IfLastModified.createAgeCondition(duration);
        }
        return IfFileName.createNameCondition("glob", LINEAGE_LOG_FILE + ".*", nestedCondition);
    }

    private boolean isPluginActivated() {
        return containsIgnoreCase(Config.activate_lineage_plugin, PLUGIN_NAME);
    }

    private DataworkLineageInfo buildLineageDetailInfo(LineageInfo lineageInfo, ScopeFlags scopeFlags) {
        boolean emitColumn = scopeFlags.emitColumn;
        LineageContext context = lineageInfo.getContext();
        DataworkLineageInfo dataworkLineageInfo = initDetailInfo(context);
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Build Dataworks lineage detail. queryId={}, emitColumn={}, targetTable={}",
                    dataworkLineageInfo.queryId, emitColumn,
                    lineageInfo.getTargetTable() == null ? "" : lineageInfo.getTargetTable().getName());
        }
        if (emitColumn) {
            dataworkLineageInfo.columnLineages = buildColumnLineages(lineageInfo);
        } else {
            dataworkLineageInfo.columnLineages = Collections.emptyList();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Build Dataworks lineage detail done. queryId={}, columnLineageSize={}",
                    dataworkLineageInfo.queryId,
                    dataworkLineageInfo.columnLineages == null ? 0 : dataworkLineageInfo.columnLineages.size());
        }
        return dataworkLineageInfo;
    }

    private DataworkLineageInfo initDetailInfo(LineageContext context) {
        DataworkLineageInfo dataworkLineageInfo = new DataworkLineageInfo();
        dataworkLineageInfo.actionType = ACTION_TYPE;
        dataworkLineageInfo.action = resolveAction(context);
        dataworkLineageInfo.state = resolveState(context == null ? null : context.getState());
        dataworkLineageInfo.queryText = Strings.nullToEmpty(context.getQueryText());
        dataworkLineageInfo.queryId = Strings.nullToEmpty(context.getQueryId());
        dataworkLineageInfo.timestamp = context.getTimestampMs();
        dataworkLineageInfo.costTime = context.getDurationMs();
        dataworkLineageInfo.clientIp = Strings.nullToEmpty(context.getClientIp());
        dataworkLineageInfo.user = Strings.nullToEmpty(context.getUser());
        dataworkLineageInfo.columnLineages = Collections.emptyList();
        return dataworkLineageInfo;
    }

    private String resolveState(String rawState) {
        String normalized = Strings.nullToEmpty(rawState).trim();
        if (normalized.isEmpty()) {
            return STATE_FAILED;
        }
        if (STATE_SUCCESS.equalsIgnoreCase(normalized)) {
            return STATE_SUCCESS;
        }
        if (STATE_FAILED.equalsIgnoreCase(normalized)) {
            return STATE_FAILED;
        }
        if (STATE_OK.equalsIgnoreCase(normalized)
                || STATE_EOF.equalsIgnoreCase(normalized)
                || STATE_NOOP.equalsIgnoreCase(normalized)) {
            return STATE_SUCCESS;
        }
        return STATE_FAILED;
    }

    private String resolveAction(LineageContext context) {
        if (context == null || context.getSourceCommand() == null) {
            return EMPTY_STRING;
        }
        return context.getSourceCommand().getSimpleName();
    }

    private List<ColumnLineage> buildColumnLineages(LineageInfo lineageInfo) {
        TableIf targetTable = lineageInfo.getTargetTable();
        if (targetTable == null) {
            LOG.error(LOG_PREFIX + "target table is null in lineage info");
            return Collections.emptyList();
        }
        TableInfo destInfo = resolveTableInfo(targetTable, "target");
        if (destInfo == null) {
            return Collections.emptyList();
        }
        Map<String, ColumnLineageBuilder> builders = new LinkedHashMap<>();
        TableInfo singleSource = resolveSingleSourceTable(lineageInfo.getTableLineageSet());
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Build column lineages start. target={}.{}.{} , tableLineageCount={}, singleSource={}",
                    destInfo.catalog, destInfo.database, destInfo.table,
                    lineageInfo.getTableLineageSet() == null ? 0 : lineageInfo.getTableLineageSet().size(),
                    singleSource == null ? "null"
                            : singleSource.catalog + "." + singleSource.database + "." + singleSource.table);
        }

        Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap =
                lineageInfo.getDirectLineageMap();
        if (directLineageMap == null || directLineageMap.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Build column lineages skipped because directLineageMap is empty");
            }
            return Collections.emptyList();
        }

        for (Map.Entry<SlotReference, SetMultimap<DirectLineageType, Expression>> entry
                : directLineageMap.entrySet()) {
            SlotReference targetSlot = entry.getKey();
            if (targetSlot == null) {
                continue;
            }
            String targetColumn = targetSlot.getName();
            boolean hasSource = false;
            for (Expression expr : entry.getValue().values()) {
                List<SlotReference> sourceSlots = collectSourceSlots(expr);
                if (sourceSlots.isEmpty()) {
                    continue;
                }
                hasSource = true;
                for (SlotReference sourceSlot : sourceSlots) {
                    TableInfo srcInfo = resolveSlotTableInfo(sourceSlot);
                    if (srcInfo == null) {
                        continue;
                    }
                    ColumnLineageBuilder builder = getOrCreateBuilder(builders, destInfo, srcInfo);
                    builder.addSource(targetColumn, sourceSlot.getName());
                }
            }
            if (!hasSource && singleSource != null) {
                ColumnLineageBuilder builder = getOrCreateBuilder(builders, destInfo, singleSource);
                builder.addEmptyTarget(targetColumn);
            } else if (!hasSource && LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "No source slots found for targetColumn='{}', and singleSource is null",
                        targetColumn);
            }
        }

        if (builders.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Build column lineages result is empty. target={}.{}.{}", destInfo.catalog,
                        destInfo.database, destInfo.table);
            }
            return Collections.emptyList();
        }
        List<ColumnLineage> lineages = new ArrayList<>(builders.size());
        for (ColumnLineageBuilder builder : builders.values()) {
            lineages.add(builder.build());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Build column lineages done. target={}.{}.{} , lineageEntryCount={}",
                    destInfo.catalog, destInfo.database, destInfo.table, lineages.size());
        }
        return lineages;
    }

    private ColumnLineageBuilder getOrCreateBuilder(Map<String, ColumnLineageBuilder> builders,
            TableInfo destInfo, TableInfo srcInfo) {
        String key = buildLineageKey(destInfo, srcInfo);
        ColumnLineageBuilder builder = builders.get(key);
        if (builder == null) {
            builder = new ColumnLineageBuilder(destInfo, srcInfo);
            builders.put(key, builder);
        }
        return builder;
    }

    private String buildLineageKey(TableInfo destInfo, TableInfo srcInfo) {
        return destInfo.catalog + HASH_SEPARATOR + destInfo.database + HASH_SEPARATOR + destInfo.table
                + HASH_SEPARATOR + srcInfo.catalog + HASH_SEPARATOR + srcInfo.database + HASH_SEPARATOR
                + srcInfo.table;
    }

    private TableInfo resolveSingleSourceTable(Set<TableIf> tables) {
        if (tables == null || tables.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Resolve single source table skipped because table lineage set is empty");
            }
            return null;
        }
        List<TableInfo> infos = new ArrayList<>();
        for (TableIf table : tables) {
            TableInfo info = resolveTableInfo(table, "source");
            if (info != null) {
                infos.add(info);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Resolve single source table result. candidateCount={}, resolvedCount={}, singleSource={}",
                    tables.size(), infos.size(), infos.size() == SINGLE_SOURCE_TABLE_COUNT);
        }
        return infos.size() == SINGLE_SOURCE_TABLE_COUNT ? infos.get(0) : null;
    }

    private TableInfo resolveSlotTableInfo(SlotReference slot) {
        if (slot == null) {
            return null;
        }
        TableIf table = slot.getOriginalTable().orElseGet(() -> slot.getOneLevelTable().orElse(null));
        if (table == null) {
            LOG.error(LOG_PREFIX + "missing table info for slot: {}", slot.toString());
            return null;
        }
        return resolveTableInfo(table, "source");
    }

    private TableInfo resolveTableInfo(TableIf table, String role) {
        if (table == null) {
            LOG.error(LOG_PREFIX + "{} table is null", role);
            return null;
        }
        DatabaseIf db = table.getDatabase();
        if (db == null) {
            LOG.error(LOG_PREFIX + "missing database for {} table: {}", role, table);
            return null;
        }
        CatalogIf catalog = db.getCatalog();
        if (catalog == null) {
            LOG.error(LOG_PREFIX + "missing catalog for {} table: {}", role, table);
            return null;
        }
        TableInfo info = new TableInfo();
        info.catalog = catalog.getName();
        info.database = normalizeDbName(db.getFullName());
        info.table = table.getName();
        return info;
    }

    private List<SlotReference> collectSourceSlots(Expression expr) {
        if (expr == null) {
            return Collections.emptyList();
        }
        List<SlotReference> slots = expr.collectToList(SlotReference.class::isInstance);
        if (slots.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "No source slot collected from expression type={}",
                        expr.getClass().getSimpleName());
            }
            return Collections.emptyList();
        }
        return slots;
    }

    private String normalizeDbName(String dbName) {
        if (Strings.isNullOrEmpty(dbName)) {
            return EMPTY_STRING;
        }
        return ClusterNamespace.getNameFromFullName(dbName);
    }

    private ScopeFlags parseScopes(String scopeValue) {
        if (Strings.isNullOrEmpty(scopeValue)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Dataworks lineage scope is empty, lineage collection disabled");
            }
            return ScopeFlags.disabled();
        }
        boolean emitTable = false;
        boolean emitColumn = false;
        for (String raw : scopeValue.split(",")) {
            String normalized = raw.trim().toLowerCase(Locale.ROOT);
            if (SCOPE_TABLE.equals(normalized)) {
                emitTable = true;
            } else if (SCOPE_COLUMN.equals(normalized)) {
                emitColumn = true;
            } else if (!Strings.isNullOrEmpty(normalized) && LOG.isDebugEnabled()) {
                LOG.debug(LOG_PREFIX + "Ignore unsupported Dataworks lineage scope token='{}' from raw='{}'",
                        normalized, raw);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(LOG_PREFIX + "Parsed Dataworks lineage scope. raw='{}', emitTable={}, emitColumn={}",
                    scopeValue, emitTable, emitColumn);
        }
        return new ScopeFlags(emitTable, emitColumn);
    }

    private boolean containsIgnoreCase(String[] values, String target) {
        if (values == null || values.length == 0) {
            return false;
        }
        for (String value : values) {
            if (value != null && target.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private static class ScopeFlags {
        private static final ScopeFlags DISABLED = new ScopeFlags(false, false);

        private final boolean emitTable;
        private final boolean emitColumn;

        private ScopeFlags(boolean emitTable, boolean emitColumn) {
            this.emitTable = emitTable;
            this.emitColumn = emitColumn;
        }

        private static ScopeFlags disabled() {
            return DISABLED;
        }

        private boolean enabled() {
            return emitTable || emitColumn;
        }
    }

    private static class TableInfo {
        private String catalog;
        private String database;
        private String table;
    }

    private static class ColumnLineageBuilder {
        private final TableInfo destInfo;
        private final TableInfo srcInfo;
        private final Map<String, LinkedHashSet<String>> columnMap = new LinkedHashMap<>();

        private ColumnLineageBuilder(TableInfo destInfo, TableInfo srcInfo) {
            this.destInfo = destInfo;
            this.srcInfo = srcInfo;
        }

        private void addSource(String targetColumn, String sourceColumn) {
            if (Strings.isNullOrEmpty(targetColumn) || Strings.isNullOrEmpty(sourceColumn)) {
                return;
            }
            LinkedHashSet<String> sources = columnMap.computeIfAbsent(targetColumn, key -> new LinkedHashSet<>());
            sources.add(sourceColumn);
        }

        private void addEmptyTarget(String targetColumn) {
            if (Strings.isNullOrEmpty(targetColumn)) {
                return;
            }
            columnMap.computeIfAbsent(targetColumn, key -> new LinkedHashSet<>());
        }

        private ColumnLineage build() {
            ColumnLineage lineage = new ColumnLineage();
            lineage.destCatalog = destInfo.catalog;
            lineage.destDatabase = destInfo.database;
            lineage.destTable = destInfo.table;
            lineage.srcCatalog = srcInfo.catalog;
            lineage.srcDatabase = srcInfo.database;
            lineage.srcTable = srcInfo.table;
            Map<String, List<String>> output = new LinkedHashMap<>();
            for (Map.Entry<String, LinkedHashSet<String>> entry : columnMap.entrySet()) {
                output.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            lineage.columnMap = output;
            return lineage;
        }
    }

    private static class DataworkLineageInfo {
        long timestamp;
        String actionType;
        String action;
        String state;
        String queryText;
        String queryId;
        long costTime;
        String clientIp;
        String user;
        List<ColumnLineage> columnLineages;
    }

    private static class ColumnLineage {
        String destCatalog;
        String srcCatalog;
        String destDatabase;
        String srcDatabase;
        String destTable;
        String srcTable;
        Map<String, List<String>> columnMap;
    }

    private static class DataworksLogConfig {
        private static final int DEFAULT_ROLL_NUM = 90;
        private static final int DEFAULT_ROLL_MAXSIZE_MB = 1024;
        private static final String DEFAULT_ROLL_INTERVAL = "DAY";
        private static final String DEFAULT_DELETE_AGE = "30d";
        private static final int DEFAULT_ACCUMULATED_SIZE_GB = 4;

        private static final String LOG_DIR_KEY = "lineage_dataworks_log_dir";
        private static final String ROLL_NUM_KEY = "lineage_dataworks_roll_num";
        private static final String ROLL_MAXSIZE_KEY = "lineage_dataworks_roll_maxsize";
        private static final String ROLL_INTERVAL_KEY = "lineage_dataworks_log_roll_interval";
        private static final String DELETE_AGE_KEY = "lineage_dataworks_log_delete_age";
        private static final String ACCUMULATED_SIZE_KEY = "lineage_dataworks_sys_accumulated_file_size";

        private String logDir = System.getenv("LOG_DIR");
        private int rollNum = DEFAULT_ROLL_NUM;
        private int rollMaxSizeMb = DEFAULT_ROLL_MAXSIZE_MB;
        private String rollInterval = DEFAULT_ROLL_INTERVAL;
        private String deleteAge = DEFAULT_DELETE_AGE;
        private int accumulatedFileSizeGb = DEFAULT_ACCUMULATED_SIZE_GB;

        private static DataworksLogConfig from(Properties properties) {
            DataworksLogConfig conf = new DataworksLogConfig();
            if (properties == null || properties.isEmpty()) {
                return conf;
            }
            conf.logDir = getString(properties, LOG_DIR_KEY, conf.logDir);
            conf.rollNum = getInt(properties, ROLL_NUM_KEY, conf.rollNum);
            conf.rollMaxSizeMb = getInt(properties, ROLL_MAXSIZE_KEY, conf.rollMaxSizeMb);
            conf.rollInterval = getString(properties, ROLL_INTERVAL_KEY, conf.rollInterval);
            conf.deleteAge = getString(properties, DELETE_AGE_KEY, conf.deleteAge);
            conf.accumulatedFileSizeGb = getInt(properties, ACCUMULATED_SIZE_KEY, conf.accumulatedFileSizeGb);
            return conf;
        }

        private String resolveRollPattern() {
            if ("HOUR".equalsIgnoreCase(rollInterval)) {
                return "%d{yyyyMMddHH}";
            }
            return "%d{yyyyMMdd}";
        }

        private static String getString(Properties properties, String key, String defaultValue) {
            String value = properties.getProperty(key);
            return Strings.isNullOrEmpty(value) ? defaultValue : value.trim();
        }

        private static int getInt(Properties properties, String key, int defaultValue) {
            String value = properties.getProperty(key);
            if (Strings.isNullOrEmpty(value)) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }
}
