<%@ page import="net.sf.json.JSONObject" %>
<%@ page import="data.sync.core.JobHistory" %>
<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title><%=request.getParameter("jobid")%>
    </title>

    <link rel="stylesheet" href="http://apps.bdimg.com/libs/bootstrap/3.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="css/self.css">
    <script src="js/jquery-1.11.3.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <link rel="stylesheet" href="bootstrap-table/bootstrap-table.css">
    <link rel="stylesheet" href="css/bootstrap-dialog.css">
    <script src="bootstrap-table/bootstrap-table.js"></script>
    <script src="bootstrap-table/locale/bootstrap-table-zh-CN.js"></script>
    <script src="js/bootstrap-dialog.js"></script>
    <script>

        var jobData = <%=
            JSONObject.fromObject(JobHistory.getMemHjob(request.getParameter("jobid"))).toString()
        %>
                $(function () {
                    $('#table').bootstrapTable({
                        data: jobData.tasks
                    });
                });
        var attemptDic = {};
        for (var i = 0; i < jobData.tasks.length; i++) {
            attemptDic[jobData.tasks[i].taskId] = jobData.tasks[i].attempts;
        }
        function runningFormatter(value, row, index) {
            return index + 1;
        }
        function showAttempt(value) {
            $('#infoTable').bootstrapTable({
                data: attemptDic[value]
            });
            $('#infoTable').bootstrapTable("load", attemptDic[value]);
            BootstrapDialog.show({
                title: "Task执行详情: " + value,
                autodestroy: false,
                message: $('#infoTable')
            });
        }
        function taskFormatter(value) {
            return '<a href="javascript:void(0)" onclick=showAttempt("' + value + '")>' + value + '</a>';
        }
    </script>
</head>
<h3>&nbsp&nbsp<span class="label label-default"><%=request.getParameter("jobid")%></span></h3>
<table
        data-page-size="10"
        data-pagination="true"
        id="table"
        data-classes="table table-hover table-condensed"
        data-striped="true"
        data-sort-name="taskId"
        data-sort-order="asc"
        >
    <thead>
    <tr>
        <th data-formatter="runningFormatter" data-sortable="true">Index</th>
        <th data-field="taskId" data-formatter="taskFormatter" data-sortable="true">TaskId</th>
        <th data-field="sql" data-sortable="true">SQL</th>
        <th data-field="table" data-sortable="true">表</th>
        <th data-field="db" data-sortable="true">库</th>
        <th data-field="startTime" data-sortable="true">开始时间</th>
        <th data-field="finishTime" data-sortable="true">结束时间</th>
        <th data-field="status" data-sortable="true">状态</th>
    </tr>
    </thead>
</table>

<div id="atttempts" style="display:none">
    <table data-card-view="true"
           data-classes="table table-hover table-condensed"
           data-striped="true"
           id="infoTable"
           data-sort-name="attemptId"
           data-sort-order="asc"
            >
        <thead>
        <tr>
            <th data-field="attemptId">AttemptId &nbsp:&nbsp</th>
            <th data-field="beeId">BeeId &nbsp:&nbsp</th>
            <th data-field="readNum">读取记录 &nbsp:&nbsp</th>
            <th data-field="writeNum">写入记录 &nbsp:&nbsp</th>
            <th data-field="bufferSize">缓存大小 &nbsp:&nbsp</th>
            <th data-field="startTime">开始时间 &nbsp:&nbsp</th>
            <th data-field="finishTime">结束时间 &nbsp:&nbsp</th>
            <th data-field="status">状态 &nbsp:&nbsp</th>
            <th data-field="error">错误 &nbsp:&nbsp</th>
        </tr>
        </thead>
    </table>
</div>
</body>
</html>