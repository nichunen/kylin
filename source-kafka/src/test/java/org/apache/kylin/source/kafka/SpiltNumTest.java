/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.source.kafka;

import org.apache.kylin.source.kafka.hadoop.KafkaInputSplit;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.collect.Maps;

public class SpiltNumTest {

    public static List<InputSplit> getSplits() {

        final String brokers = "brokers";
        final String inputTopic = "topic";
        final Integer spiltsSetnum = 10;

        final Map<Integer, Long> startOffsetMap = Maps.newHashMap();
        final Map<Integer, Long> endOffsetMap = Maps.newHashMap();
        startOffsetMap.put(0, Long.valueOf(0));
        endOffsetMap.put(0, Long.valueOf(15));
        startOffsetMap.put(1, Long.valueOf(4));
        endOffsetMap.put(1, Long.valueOf(26));
        startOffsetMap.put(2, Long.valueOf(15));
        endOffsetMap.put(2, Long.valueOf(47));
        startOffsetMap.put(3, Long.valueOf(39));
        endOffsetMap.put(3, Long.valueOf(41));

        final List<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < 4; i++) {
                int partitionId = i;
                long new_start = startOffsetMap.get(partitionId);
                long end = endOffsetMap.get(partitionId);
                while (end > new_start) {
                    if ((end - new_start) <= spiltsSetnum && (end > new_start)) {
                        InputSplit split = new KafkaInputSplit(brokers, inputTopic, partitionId, new_start, end);
                        splits.add(split);
                        break;
                    } else {
                        InputSplit split = new KafkaInputSplit(brokers, inputTopic, partitionId, new_start, new_start + spiltsSetnum);
                        splits.add(split);
                        new_start += spiltsSetnum;
                    }
                }
            }
        return splits;
    }

    @Test
    public void testSpiltNum(){
        int slen = 0;
        List<InputSplit> splits = getSplits();
        slen = splits.size();
        Assert.assertEquals(slen, 10);
    }

    @Test
    public void testSpilt(){
        boolean flag = false;
        boolean flag1 = false;
        boolean flag2 = false;
        boolean flag3 = false;
        boolean flag4 = false;
        boolean flag5 = false;
        boolean flag6 = false;
        boolean flag7 = false;
        boolean flag8 = false;
        boolean flag9 = false;
        boolean flag10 = false;
        boolean result = false;
        List<InputSplit> splits = getSplits();
        for(Object eachspilt : splits){
            flag = eachspilt.toString().contains("brokers-topic-0-0-10");
            if(flag){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag1 = eachspilt.toString().contains("brokers-topic-0-10-15");
            if(flag1){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag2 = eachspilt.toString().contains("brokers-topic-1-4-14");
            if(flag2){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag3 = eachspilt.toString().contains("brokers-topic-1-14-24");
            if(flag3){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag4 = eachspilt.toString().contains("brokers-topic-1-24-26");
            if(flag4){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag5 = eachspilt.toString().contains("brokers-topic-2-15-25");
            if(flag5) {
                break;
            }
        }
        for(Object eachspilt : splits){
            flag6 = eachspilt.toString().contains("brokers-topic-2-25-35");
            if(flag6){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag7 = eachspilt.toString().contains("brokers-topic-2-35-45");
            if(flag7){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag8 = eachspilt.toString().contains("brokers-topic-2-45-47");
            if(flag8){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag9 = eachspilt.toString().contains("brokers-topic-3-39-41");
            if(flag9){
                break;
            }
        }
        for(Object eachspilt : splits){
            flag10 = eachspilt.toString().contains("brokers-topic-0-4-47");
            if(flag10){
                break;
            }
        }
        result = flag && flag1 && flag2 && flag3 && flag4 && flag5 && flag6 && flag7 && flag8 && flag9;
        Assert.assertTrue(result);
        Assert.assertNotEquals(flag10, true);
    }
}
