package com.wx.mystorm.customgroup;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author weixing
 * @date 2019/3/3
 **/
public class MyCustomGroup implements CustomStreamGrouping {

    private Integer max;

    private List<Integer> list;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.list = list;
        Collections.sort(this.list);
        max = this.list.get(this.list.size() - 1);
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        return Arrays.asList(max);
    }
}
