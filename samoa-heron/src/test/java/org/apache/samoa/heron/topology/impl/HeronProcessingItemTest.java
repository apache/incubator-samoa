package org.apache.samoa.heron.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.junit.Assert.assertEquals;

import mockit.Expectations;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;

import org.apache.samoa.core.Processor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.samoa.heron.topology.impl.HeronProcessingItem;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;

public class HeronProcessingItemTest {
    private static final int PARRALLELISM_HINT_2 = 2;
    private static final int PARRALLELISM_HINT_4 = 4;
    private static final String ID = "id";
    @Tested
    private HeronProcessingItem pi;
    @Mocked
    private Processor processor;
    @Mocked
    private HeronTopology topology;
    @Mocked
    private TopologyBuilder heronBuilder = new TopologyBuilder();

    @Before
    public void setUp() {
        pi = new HeronProcessingItem(processor, ID, PARRALLELISM_HINT_2);
    }

    @Test
    public void testAddToTopology() {
        new Expectations() {
            {
                topology.getHeronBuilder();
                result = heronBuilder;

                heronBuilder.setBolt(ID, (IRichBolt) any, anyInt);
                result = new MockUp<BoltDeclarer>() {
                }.getMockInstance();
            }
        };

        pi.addToTopology(topology, PARRALLELISM_HINT_4); // this parallelism hint is ignored

        new Verifications() {
            {
                assertEquals(pi.getProcessor(), processor);
                // TODO add methods to explore a topology and verify them
                assertEquals(pi.getParallelism(), PARRALLELISM_HINT_2);
                assertEquals(pi.getId(), ID);
            }
        };
    }
}
