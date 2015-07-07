<%@ page import="data.sync.core.Queen" %>
<%@ page import="data.sync.common.Constants" %>
<%@ page import="data.sync.core.Queen$" %>
<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8" %>
<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title>Honeycombx-<%=Queen.state()%></title>

    <link rel="stylesheet" href="http://apps.bdimg.com/libs/bootstrap/3.3.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="css/self.css">
    <script src="js/jquery-1.11.3.min.js"></script>
    <script src="js/bootstrap.min.js"></script>
    <link rel="stylesheet" href="bootstrap-table/bootstrap-table.css">
    <script src="bootstrap-table/bootstrap-table.js"></script>
    <script src="bootstrap-table/locale/bootstrap-table-zh-CN.js"></script>
    <script>
        function runningFormatter(value, row, index) {
            return index + 1;
        }
        function jobFormatter(value) {
            return '<a href="javascript:void(0)" onclick=window.open("/jobinfo.jsp?jobid=' + value + '")>' + value + '</a>';
            //return '<a href="/jobinfo.jsp?jobid=' + value + '">' + value + '</a>';
        }
    </script>
</head>
<body class="home-template">
<nav class="navbar navbar-inverse" role="navigation">
    <!--<div class="navbar-header">-->
        <!--<a class="navbar-brand" href="#">W3Cschool</a>-->
    <!--</div>-->
    <div>
        <ul id="myTab" class="nav nav-pills">
            <!--<li class="active">-->
            <!--<a href="#home" data-toggle="tab">-->
            <!--首页-->
            <!--</a>-->
            <!--</li>-->
            <li class="active">
                <a href="#job" data-toggle="tab">
                    Job信息
                </a>
            </li>
            <li >
                <a href="#bee" data-toggle="tab">
                    Bee信息
                </a>
            </li>
        </ul>
    </div>
</nav>
<div id="myTabContent" class="tab-content">
    <!--<div class="tab-pane fade in active" id="home">-->

    <!--</div>-->
    <div class="tab-pane fade" id="bee">
        <h3>Bee信息</h3>
        <table data-toggle="table"
               data-url="/rest/bee/allbees"
               data-page-size="10"
               data-pagination="true"
               data-classes="table table-hover table-condensed"
               data-striped="true"
               data-sort-name="beeId"
               data-sort-order="desc"

                >
            <thead>
            <tr>
                <th data-formatter="runningFormatter">Index</th>
                <th data-field="beeId" data-sortable="true">BeeId</th>
                <th data-field="runningWorker" data-sortable="true">运行中的Worker数</th>
                <th data-field="totalWorker" data-sortable="true">总Worker数</th>
            </tr>
            </thead>
        </table>
        <h3>Bee作业分配信息</h3>
        <table data-toggle="table"
               data-url="/rest/bee/beeinfos"
               data-pagination="true"
               data-page-size="20"
               data-classes="table table-hover table-condensed"
               data-striped="true"
               data-sort-name="beeId"
               data-sort-order="desc">
            <thead>
            <tr>
                <th data-field="beeId" data-sortable="true">BeeId</th>
                <th data-field="attemptId" data-sortable="true">AttemptId</th>
                <th data-field="readNum" data-sortable="true">读取记录数</th>
                <th data-field="writeNum" data-sortable="true">写入记录数</th>
                <th data-field="bufferSize" data-sortable="true">缓存大小</th>
                <th data-field="startTime" data-sortable="true">开始时间</th>
                <th data-field="status" data-sortable="true">状态</th>
            </tr>
            </thead>
        </table>


    </div>
    <div class="tab-pane fade in active" id="job">
        <h3>当前Job信息(状态:<%=Queen.state()%>)</h3>
        <table data-toggle="table"
               data-url="/rest/job/alljobs"
               data-page-size="10"
               data-pagination="true"
               data-classes="table table-hover table-condensed"
               data-striped="true"
               data-sort-name="startTime"
               data-sort-order="desc"
               data-show-columns="true"
                >
            <thead>
            <tr>
                <th data-field="jobId" data-formatter="jobFormatter" data-sortable="true">JobId</th>
                <th data-field="jobName" data-sortable="true">作业名</th>
                <th data-field="jobDesc" data-sortable="true">描述</th>
                <th data-field="priority" data-visible="false">优先级</th>
                <th data-field="appendTasks" data-sortable="true">挂起任务</th>
                <th data-field="runningTasks" data-sortable="true">运行任务</th>
                <th data-field="finishedTasks" data-sortable="true">结束任务</th>
                <th data-field="failedTasks" data-sortable="true">失败任务</th>
                <th data-field="startTime" data-sortable="true">提交时间</th>
                <th data-field="targetDir" data-visible="false">输出路径</th>
                <th data-field="cmd"  data-visible="false">回调命令</th>
                <th data-field="url" data-visible="false">通知接口</th>
                <th data-field="user">用户</th>
                <th data-field="status" data-sortable="true">状态</th>
            </tr>
            </thead>
        </table>
        <h3>历史Job信息</h3>
        <table data-toggle="table"
               data-url="/rest/job/hisjobs"
               data-pagination="true"
               data-page-size="20"
               data-classes="table table-hover table-condensed"
               data-striped="true"
               data-sort-name="startTime"
               data-sort-order="desc"
               data-show-columns="true">
            <thead>
            <tr>
                <th data-field="jobId" data-formatter="jobFormatter" data-sortable="true">JobId</th>
                <th data-field="jobDesc" data-sortable="true">描述</th>
                <th data-field="priority" data-sortable="true">优先级</th>
                <th data-field="targetDir" data-sortable="true">输出路径</th>
                <th data-field="startTime" data-sortable="true">开始时间</th>
                <th data-field="finishedTime" data-sortable="true">结束时间</th>
                <th data-field="cmd" data-visible="false">回调命令</th>
                <th data-field="url" data-visible="false">通知接口</th>
                <th data-field="user" data-sortable="true">用户</th>
                <th data-field="jobName" data-sortable="true">作业名</th>
                <th data-field="status" data-sortable="true">状态</th>

            </tr>
            </thead>
        </table>
    </div>
</div>

</body>
</html>