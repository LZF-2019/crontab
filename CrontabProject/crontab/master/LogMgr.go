package master

import (
	"CrontabProject/crontab/common"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//mongodb日志管理
type LogMgr struct{
	client *mongo.Client
	logCollection *mongo.Collection
}

var(
	G_logMgr *LogMgr
)

func InitLogMgr() (err error){
	var(
		client *mongo.Client
		clientOptions *options.ClientOptions
	)

	//建立mongo连接
	clientOptions = options.Client().ApplyURI(G_config.MongodbUri)
	// 1. 建立连接
	client, err = mongo.Connect(context.TODO(), clientOptions)
	if err!=nil{
		//log.Fatal(err)
		return
	}
	err = client.Ping(context.TODO(), nil)
	if err!=nil{
		//log.Fatal(err)
		return
	}

	G_logMgr = &LogMgr{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

//查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int)(logArr []*common.JobLog, err error){
	var(
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)

	//len(logArr)=0 这里进行初始化是为了防止没有查询到值，这样外部函数可以直接通过len来判断是否有数据
	//而不用担心logArr会返回一个空指针回去
	logArr = make([]*common.JobLog, 0)

	//过滤条件
	filter = &common.JobLogFilter{JobName: name}

	//按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	//查询
	if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, options.Find().SetSort(logSort),options.Find().SetSkip(int64(skip)), options.Find().SetLimit(int64(limit))); err!=nil{
		fmt.Println(err)
		return
	}
	//延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()){
		jobLog = &common.JobLog{}

		//反序列化BSON
		if err = cursor.Decode(jobLog); err!=nil{
			continue //日志不合法
		}

		logArr = append(logArr, jobLog)
	}
	return
}

// 删除任务日志
func (logMgr *LogMgr) DeleteLog(name string)(err error){
	var(
		filter *common.JobLogFilter
	)
	//过滤条件
	filter = &common.JobLogFilter{JobName: name}

	if _, err = logMgr.logCollection.DeleteMany(context.TODO(), filter); err!=nil{
		fmt.Println(err)
		return
	}
	return
}