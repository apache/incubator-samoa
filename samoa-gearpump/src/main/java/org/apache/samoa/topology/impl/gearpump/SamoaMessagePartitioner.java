package org.apache.samoa.topology.impl.gearpump;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
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

import org.apache.gearpump.Message;
import org.apache.gearpump.partitioner.BroadcastPartitioner;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.MulticastPartitioner;
import org.apache.gearpump.partitioner.ShufflePartitioner;

public class SamoaMessagePartitioner implements MulticastPartitioner {
  ShufflePartitioner shufflePartitioner = new ShufflePartitioner();
  BroadcastPartitioner broadcastPartitioner = new BroadcastPartitioner();
  HashPartitioner hashPartitioner = new HashPartitioner();

  @Override
  public int[] getPartitions(Message msg, int partitionNum, int currentPartitionId) {
    GearpumpMessage message = (GearpumpMessage) msg.msg();
    int[] partitions = null;
    switch (message.getScheme()) {
      case SHUFFLE:
        partitions = new int[]{
            shufflePartitioner.getPartition(msg, partitionNum, currentPartitionId)
        };
        break;
      case BROADCAST:
        partitions = broadcastPartitioner.getPartitions(msg, partitionNum);
        break;
      case GROUP_BY_KEY:
        partitions = new int[]{
            hashPartitioner.getPartition(msg, partitionNum, currentPartitionId)
        };
        break;
    }
    return partitions;
  }

  @Override
  public int[] getPartitions(Message msg, int partitionNum) {
    return this.getPartitions(msg, partitionNum, -1);
  }
}
