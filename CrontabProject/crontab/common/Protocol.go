package common

import (
	"context"
	"encoding/json"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 定时任务
type Job struct {
	Name string `json:"name"` //任务名
	Command string `json:"command"` //shell命令
	CronExpr string `json:"cronExpr"` //cron表达式
	WorkIp string `json:"workIp"` //自定义调度节点IP
}

// 任务调度计划
type JobSchedulerPlan struct {
	Job *Job //要调度的任务信息
	Expr *cronexpr.Expression //解析好的cronexpr表达式
	NextTime time.Time //下次调度时间
}

//任务执行状态
type JobExecuteInfo struct {
	Job *Job //任务信息
	PlanTime time.Time //理论上的调度时间
	RealTime time.Time //实际的调度时间
	CancelCtx context.Context //任务command的context
	CancelFunc context.CancelFunc //用于取消command执行的cancel函数
}

// HTTP接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 变化事件
type JobEvent struct{
	EventType int // SAVE, DELETE
	Job *Job
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo //执行状态
	Output []byte //脚本输出
	Err error //脚本错误原因
	StartTime time.Time //脚本启动时间
	EndTime time.Time //结束时间
	WorkIp string //任务调度IP
}

// 任务执行日志
type JobLog struct {
	JobName string `json:"jobName" bson:"jobName"`  //任务名字
	Command string `json:"command" bson:"command"` //脚本命令
	Err string `json:"err" bson:"err"` //错误原因
	Output string `json:"output" bson:"output"` //脚本输出
	PlanTime int64 `json:"planTime" bson:"planTime"` //计划开始时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"` //实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"` //任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"` //任务执行结束时间
	WorkIp string `json:"workIp" bson:"workIp"` //任务调度所在节点IP
}

//日志批次
type LogBatch struct{
	Logs []interface{} //多条日志
}

//任务日志过滤条件
type JobLogFilter struct{
	JobName string `bson:"jobName"`
}

//任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` //{startTime:-1}
}

// 应答方法
func BuildResponse(errno int, msg string, data interface{})(resp []byte, err error){
	// 1.定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 2.序列化json
	resp, err = json.Marshal(response)
	return
}

// 反序列化Job
func UnpackJob(value []byte)(ret *Job, err error){
	var(
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err!=nil{
		return
	}
	ret = job
	return
}

// 从etcd的key中提取任务名
// 从/cron/jobs/job10抹掉/cron/jobs/
func ExtractJobName(jobKey string) (string){
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 从/cron/killer/job10抹掉/cron/killer/
func ExtractKillerName(killerKey string) (string){
	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}

//提取worker的ip
func ExtractWorkerIP(regKey string) (string){
	return strings.TrimPrefix(regKey, JOB_WORKER_DIR)
}

// 任务变化事件有两种 1）更新任务 2）删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent){
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

//构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulerPlan*JobSchedulerPlan, err error){
	var(
		expr *cronexpr.Expression
	)
	//解析Job的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err!=nil{
		return
	}
	//生成任务调度计划对象
	jobSchedulerPlan = &JobSchedulerPlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//构造执行状态
func BuildJobExecuteInfo(jobSchedulerPlan *JobSchedulerPlan) (jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job: jobSchedulerPlan.Job,
		PlanTime: jobSchedulerPlan.NextTime, //计算调度时间
		RealTime: time.Now(), //真实调度时间
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}