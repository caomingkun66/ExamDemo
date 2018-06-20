package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import com.sun.deploy.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
*类名和方法不能修改
 */
public class Schedule {

    //服务节点Map<nodeId,List<taskId>>
    Map<Integer, List<Integer>> nodeMap;

    //任务Map<taskId,consumption>
    Map<Integer, Integer> taskMap;

    //任务执行Map<taskId,List<nodeId,consumption>>
    Map<Integer, List<Integer>> taskExecuteMap;

    // 2.1系统初始化
    public int init() {
        nodeMap = new HashMap<Integer, List<Integer>>();
        taskMap = new HashMap<Integer, Integer>();
        taskExecuteMap = new HashMap<Integer, List<Integer>>();
        return ReturnCodeKeys.E001;
    }

    // 2.2服务节点注册
    public int registerNode(int nodeId) {
        if (nodeId > 0) {
            if (nodeMap.get(nodeId) == null) {
                // 服务节点注册成功
                nodeMap.put(nodeId,new ArrayList<Integer>());
                return ReturnCodeKeys.E003;
            } else {
                // 服务节点已注册
                return ReturnCodeKeys.E005;
            }
        } else {
            // 服务节点编号非法
            return ReturnCodeKeys.E004;
        }
    }

    // 2.3服务节点注销
    public int unregisterNode(int nodeId) {
        if (nodeId > 0) {
            if (nodeMap.get(nodeId) == null) {
                // 服务节点不存在
                return ReturnCodeKeys.E007;
            } else {
                // 服务节点注销,判断任务调度情况
                List<Integer> taskIds = nodeMap.get(nodeId);
                if (taskIds.size() > 0) {
                    for (int i = 0; i < taskIds.size(); i++) {
                        int taskId = taskIds.get(i);
                        if (taskExecuteMap.get(taskId) != null) {
                            taskMap.put(taskId, taskExecuteMap.get(taskId).get(1));
                            taskExecuteMap.remove(taskId);
                        }
                    }
                    nodeMap.remove(nodeId);
                }
                return ReturnCodeKeys.E006;
            }
        } else {
            // 服务节点编号非法
            return ReturnCodeKeys.E004;
        }
    }

    // 2.4添加任务
    public int addTask(int taskId, int consumption) {
        if (taskId > 0) {
            if (taskMap.get(taskId) == null && taskExecuteMap.get(taskId) == null) {
                // 添加任务
                taskMap.put(taskId, consumption);
                return ReturnCodeKeys.E008;
            } else {
                // 任务已添加
                return ReturnCodeKeys.E010;
            }
        } else {
            // 任务编号非法
            return ReturnCodeKeys.E009;
        }
    }

    // 2.5删除任务
    public int deleteTask(int taskId) {
        if(taskId > 0){
            if (taskMap.get(taskId) == null  && taskExecuteMap.get(taskId) == null) {
                // 任务不存在
                return ReturnCodeKeys.E012;
            } else {
                // 删除任务
                if (taskMap.get(taskId) != null) {
                    taskMap.remove(taskId);
                } else if (taskExecuteMap.get(taskId) != null) {
                    nodeMap.remove(taskExecuteMap.get(taskId).get(0));
                    taskExecuteMap.remove(taskId);
                }
                return ReturnCodeKeys.E011;
            }
        }else {
            return ReturnCodeKeys.E009;
        }
    }

    // 2.6任务调度
    public int scheduleTask(int threshold) {
        // TODO 方法未实现
        if (!taskMap.isEmpty()){

        }else {

        }

        return ReturnCodeKeys.E000;
    }

    // 2.7查询任务状态列表
    public int queryTaskStatus(List<TaskInfo> tasks) {
        // TODO 方法未实现
        tasks = new ArrayList<TaskInfo>();
        // 遍历所有任务
        for (Integer taskId : taskMap.keySet()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setNodeId(-1);
            tasks.add(taskInfo);
        }
        // 遍历所有执行的任务
        for (Integer taskId : taskExecuteMap.keySet()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(taskId);
            taskInfo.setNodeId(taskExecuteMap.get(taskId).get(0));
            tasks.add(taskInfo);
        }
        return ReturnCodeKeys.E000;
    }

}
