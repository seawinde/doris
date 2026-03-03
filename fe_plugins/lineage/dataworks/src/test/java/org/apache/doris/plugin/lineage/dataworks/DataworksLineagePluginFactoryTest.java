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

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.nereids.lineage.LineagePluginFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.ServiceLoader;

/**
 * Unit tests for {@link DataworksLineagePluginFactory} SPI registration
 * and plugin creation.
 */
public class DataworksLineagePluginFactoryTest {

    @Test
    public void testFactoryName() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        Assert.assertEquals("dataworks", factory.name());
    }

    @Test
    public void testFactoryCreate() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        LineagePlugin plugin = factory.create();

        Assert.assertNotNull("Factory should create a non-null plugin", plugin);
        Assert.assertTrue("Created plugin should be DataworksLineagePlugin",
                plugin instanceof DataworksLineagePlugin);
    }

    @Test
    public void testCreatedPluginName() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        LineagePlugin plugin = factory.create();

        Assert.assertEquals("dataworks", plugin.name());
    }

    @Test
    public void testCreatedPluginEventFilterDefaultFalse() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        LineagePlugin plugin = factory.create();

        // Without activation config, eventFilter should return false
        Assert.assertFalse("eventFilter should return false by default (no config)",
                plugin.eventFilter());
    }

    @Test
    public void testServiceLoaderDiscovery() {
        // This test verifies the META-INF/services/ SPI registration is correct
        ServiceLoader<LineagePluginFactory> factories = ServiceLoader.load(LineagePluginFactory.class);
        boolean found = false;
        for (LineagePluginFactory factory : factories) {
            if ("dataworks".equals(factory.name())) {
                found = true;
                Assert.assertTrue("ServiceLoader discovered factory should create DataworksLineagePlugin",
                        factory.create() instanceof DataworksLineagePlugin);
                break;
            }
        }
        Assert.assertTrue("DataworksLineagePluginFactory should be discoverable via ServiceLoader", found);
    }

    @Test
    public void testFactoryImplementsInterface() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        Assert.assertTrue("Factory should implement LineagePluginFactory",
                factory instanceof LineagePluginFactory);
    }

    @Test
    public void testMultipleCreateCallsReturnDifferentInstances() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        LineagePlugin plugin1 = factory.create();
        LineagePlugin plugin2 = factory.create();

        Assert.assertNotNull(plugin1);
        Assert.assertNotNull(plugin2);
        Assert.assertNotSame("Each create() call should return a new instance",
                plugin1, plugin2);
    }

    @Test
    public void testFactoryCreateWithContext() {
        DataworksLineagePluginFactory factory = new DataworksLineagePluginFactory();
        Object plugin = factory.create(new PluginContext(Collections.singletonMap("plugin.name", "dataworks")));
        Assert.assertNotNull(plugin);
        Assert.assertTrue(plugin instanceof DataworksLineagePlugin);
    }
}
