package org.apache.samoa.instances;

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

import org.apache.samoa.instances.ArffLoader;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstanceInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class ArffLoaderTest {

  private ArffLoader loader;

  private StringReader reader;

  @Before
  public void setUp() {
    String inputString = "@relation test.txt\n"
                         + "\n"
                         + "@attribute Dur numeric\n"
                         + "@attribute Proto {udp,tcp,icmp,arp,ipx/spx,ipv6-icmp,pim,esp,igmp,rtcp,rtp,ipv6,udt}\n"
                         + "@attribute Dir {' <->',' <?>',' ->',' ?>',' who',' <-',' <?'}\n"
                         + "@attribute State {CON,PA_PA,PA_FRA, ...}\n"
                         + "@attribute sTos numeric\n"
                         + "@attribute dTos numeric\n"
                         + "@attribute TotPkts numeric\n"
                         + "@attribute TotBytes numeric\n"
                         + "@attribute SrcBytes numeric\n"
                         + "@attribute class {Background,Normal,Botnet}\n"
                         + "\n"
                         + "@data\n"
                         + "\n"
                         + "1065.731934,udp,' <->',...,0,0,2,252,145,Background\n"
                         + "1471.787109,udp,' <->',CON,0,0,2,252,145,Background";
    reader = new StringReader(inputString);
    int size = 0;
    int classAttribute = 10;
    loader = new ArffLoader(reader, size, classAttribute);

  }

  @Test
  public void testGetHeader() {
    InstanceInformation header = loader.getStructure();
    assertEquals(10, header.numAttributes());
    assertEquals(9, header.classIndex());
    assertEquals(true, header.attribute(0).isNumeric());
    assertEquals(false, header.attribute(1).isNumeric());
    assertEquals(false, header.attribute(2).isNumeric());
    assertEquals(false, header.attribute(3).isNumeric());
    assertEquals(true, header.attribute(4).isNumeric());
    assertEquals(true, header.attribute(5).isNumeric());
    assertEquals(true, header.attribute(6).isNumeric());
    assertEquals(true, header.attribute(7).isNumeric());
    assertEquals(true, header.attribute(8).isNumeric());
    assertEquals(false, header.attribute(9).isNumeric());

    assertEquals(7, header.attribute(2).numValues());
    assertEquals(" <->", header.attribute(2).value(0));
    assertEquals(" <?>", header.attribute(2).value(1));
    assertEquals(" ->", header.attribute(2).value(2));
    assertEquals(" ?>", header.attribute(2).value(3));
    assertEquals(" who", header.attribute(2).value(4));
    assertEquals(" <-", header.attribute(2).value(5));
    assertEquals(" <?", header.attribute(2).value(6));

    assertEquals(3, header.attribute(9).numValues());
    assertEquals("Background", header.attribute(9).value(0));
    assertEquals("Normal", header.attribute(9).value(1));
    assertEquals("Botnet", header.attribute(9).value(2));

  }

  @Test
  public void testReadInstance() {
    Instance instance = loader.readInstance(reader);
    assertEquals(1065.731934, instance.value(0), 0);
    assertEquals(0, instance.value(1), 0);
    assertEquals(0, instance.value(2), 0);
    assertEquals(3, instance.value(3), 0);
    assertEquals(0, instance.value(4), 0);
    assertEquals(0, instance.value(5), 0);
    assertEquals(2, instance.value(6), 0);
    assertEquals(252, instance.value(7), 0);
    assertEquals(145, instance.value(8), 0);
    assertEquals(0, instance.value(9), 0);
  }
}
