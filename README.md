Honeycombx数据同步系统

技术栈：
	语言：Java，Scala
	其它：akka，jetty，curator，bootstrap
布署与启动
	目录说明：
$HONEY_HOME/conf下为系统配置
$HONEY_HOME/conf/supervisor-conf下为监控配置
$HONEY_HOME/lib为依赖包
$HONEY_HOME/bin下为命令脚本
$HONEY_HOME/web下为相关ui展现页面
同时在配置中需要指定history与log目录
	命令说明：
 start-supervisor.sh启动监控
 stop-supervisor.sh 结束监控
  deamons.sh start Queen; deamons.sh start Bee :Queen与Bee的启动
  deamons.sh stop Queen; deamons.sh stop Bee :Queen与Bee的停止
 	 honey 作业管理脚本，详细查看honey -h

客户端
两种方式提交作业：
	API方式
object HoneyClient {
	def submitJobToQueen(job: SubmitJob): String
    	def killJob(kill:KillJob):String
}

	Cli方式

./honey

Usage: honey -submit [options]
Usage: honey -kill [job ID]

Options:
  --output  job-output-path
  --number  job-task-number
  --conf    job-conf-path
  --name    job-name
  --help, -h
作业
	Job
一次同步任务的描述
	Task
Job在提交后会被分成多个task，类似MR 体系中的Map
	Attempt
每个task被分到Bee上的一次执行，当这个Bee执行失败时会在其它Bee上再起一个task的attempt,所以一个task可能有多个attempt生成

系统 
	Queen（进程）
承担master的角色，负责客户端作业的提交，作业的分片，作业运行时的管理，Bee的管理。
	Bee（进程）
扮演Slave的角色，负责任务的执行，即Attempt分被分到Bee上进行实际执行
	JobHistory
对历史作业的管理
	Supervisor（进程）
当作业开始执行与执行结束后会向Supervisor汇报，该模块中可以实现报警，重试等。
	JobManager
作业的管理，作业运行时任务的重试，状态的变化等，目前有task的重试执行，task超时的并行执行。
	BeeManager
负责Bee的注册状态维护等
	Scheduler
负责作业的调度，现在只提供基于优先级的FIFO调度,对资源的分配采取最闲Bee优先分配的策略。
	UI
对Bee与作业提供监控UI

协作
数据的同步一般为一个ETL的初始点，对于作业的触发可以使用crontab进行调度，也可以基于api方式与oozie等工作流系统集成。
当作业执行完成会时有多种方式进行获知：
1.	完成的作业输出目录下会有_SUCCESS文件生成
2.	作业提交时可以指定一个回调接口，当作业状态发生变化时会通知该接口
3.	作业提交时可以指定一个完成后执行的脚本
4.	可以通过系统提供的restful接口实时查询作业状态

UI
WebUI主要由以下三个页面组成
	Job页面
 
	Job详情页面（即该Job产生的各task）
 
	Bee信息
 
HA
	Queen存在单点故障，借助ZK实现Queen的HA.
	目前只实现最大粒度的HA,即到作业级别的。
	用户可以起动多个Queen,但只有一个Queen会被选为leader,客户端与Bee都通过探测的方式与当前leader进行连接，当作业提交时，该作业会被记录到ZK上，在执行完成后被清除，当leader宕机后，会有其它Queen被选为leader,同时客户端与Bee都会与新的leader进行连接，同时新的leader会将未执行完的Job进行恢复。



