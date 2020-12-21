/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.gpw.groovy;

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.DataWorkerData;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import groovy.lang.GroovyShell;
import groovy.lang.MetaMethod;
import groovy.lang.Script;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class GroovyDataWorkerScriptRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GroovyDataWorker.class);
    public static final String CONFIG_SCRIPT_DIR = GroovyDataWorker.class.getName() + ".scriptDir";
    public static final String CONFIG_REFRESH_INTERVAL_MS = GroovyDataWorker.class.getName() + ".refreshIntervalMs";
    public static final int CONFIG_REFRESH_INTERVAL_MS_DEFAULT = 1000;
    private final long refreshInterval;
    private final File scriptDir;
    private long lastRefreshTime;
    private Map<File, ScriptData> scripts = new HashMap<>();

    @Inject
    public GroovyDataWorkerScriptRepository(Configuration configuration) {
        refreshInterval = configuration.getInt(CONFIG_REFRESH_INTERVAL_MS, CONFIG_REFRESH_INTERVAL_MS_DEFAULT);
        String scriptDirStr = configuration.get(CONFIG_SCRIPT_DIR, null);
        checkNotNull(scriptDirStr, CONFIG_SCRIPT_DIR + " is required configuration parameter");
        scriptDir = new File(scriptDirStr);
    }

    public void refreshScripts() {
        if (!isReadyToRefresh()) {
            return;
        }
        List<File> filesToRemove = new ArrayList<>(scripts.keySet());
        refreshScripts(filesToRemove, scriptDir);
        removeScriptsThatAreNoLongerPresent(filesToRemove);
        lastRefreshTime = System.currentTimeMillis();
    }

    private void removeScriptsThatAreNoLongerPresent(List<File> filesToRemove) {
        for (File f : filesToRemove) {
            LOGGER.info("removing script: %s", f.getAbsolutePath());
            scripts.remove(f);
        }
    }

    private boolean isReadyToRefresh() {
        return lastRefreshTime + refreshInterval <= System.currentTimeMillis();
    }

    private void refreshScripts(List<File> filesToRemove, File file) {
        if (file.isFile()) {
            if (!file.getAbsolutePath().endsWith(".groovy")) {
                return;
            }
            try {
                refreshScript(file);
                filesToRemove.remove(file);
            } catch (Exception e) {
                LOGGER.error("Could not load script: " + file.getAbsolutePath(), e);
            }
        } else if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children == null) {
                return;
            }
            for (File child : children) {
                refreshScripts(filesToRemove, child);
            }
        }
    }

    private void refreshScript(File file) throws IOException {
        ScriptData scriptData = scripts.get(file);
        if (!isNew(file, scriptData)) {
            return;
        }
        scriptData = loadScript(file);
        scripts.put(file, scriptData);
    }

    private ScriptData loadScript(File file) throws IOException {
        ScriptData scriptData;
        LOGGER.info("Loading %s", file.getAbsolutePath());
        GroovyShell shell = new GroovyShell();
        Script script = shell.parse(file);
        validateScript(script);
        scriptData = new ScriptData(file, script);
        return scriptData;
    }

    private boolean isNew(File file, ScriptData scriptData) {
        if (scriptData == null) {
            return true;
        }
        return scriptData.getModifiedTime() < file.lastModified();
    }

    private void validateScript(Script script) {
        MetaMethod isHandledMethod = script.getMetaClass().getMetaMethod("isHandled", new Object[]{Element.class, Property.class});
        checkNotNull(isHandledMethod, "Could not find isHandled(Element, Property) method in script");
        MetaMethod executeMethod = script.getMetaClass().getMetaMethod("execute", new Object[]{InputStream.class, DataWorkerData.class});
        checkNotNull(executeMethod, "Could not find execute(InputStream, GraphPropertyWorkData) method in script");
    }

    public Iterable<ScriptData> getScriptDatas() {
        return this.scripts.values();
    }
}
