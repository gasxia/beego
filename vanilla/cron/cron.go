package cron

import (
	"context"
	"fmt"
	"github.com/kfchen81/beego"
	"github.com/kfchen81/beego/orm"
	"github.com/kfchen81/beego/toolbox"
	"github.com/kfchen81/beego/vanilla"
	"runtime/debug"
	"time"
)

type CronTask struct {
	name string
	spec string
	taskFunc toolbox.TaskFunc
	onlyRunThisTask bool
}

func (this *CronTask) OnlyRun() {
	this.onlyRunThisTask = true
}

var name2task = make(map[string]*CronTask)

func newTaskCtx() *TaskContext{
	inst := new(TaskContext)
	ctx := context.Background()
	o := orm.NewOrm()
	ctx = context.WithValue(ctx, "orm", o)
	resource := GetManagerResource(ctx)
	ctx = context.WithValue(ctx, "jwt", resource.CustomJWTToken)
	userId, authUserId, _ := vanilla.ParseUserIdFromJwtToken(resource.CustomJWTToken)
	ctx = context.WithValue(ctx, "user_id", userId)
	ctx = context.WithValue(ctx, "uid", authUserId)
	ctx = context.WithValue(ctx, "controller", "cron")
	resource.Ctx = ctx
	inst.Init(ctx, o, resource)
	return inst
}

func taskWrapper(task taskInterface) toolbox.TaskFunc{

	return func() error{
		taskCtx := newTaskCtx()
		o := taskCtx.GetOrm()
		ctx := taskCtx.GetCtx()

		defer vanilla.RecoverFromCronTaskPanic(ctx)
		var fnErr error
		taskName := task.GetName()
		startTime := time.Now()
		beego.Info(fmt.Sprintf("[%s] run...", taskName))
		if task.IsEnableTx(){
			o.Begin()
			fnErr = task.Run(taskCtx)
			o.Commit()
		}else{
			fnErr = task.Run(taskCtx)
		}
		dur := time.Since(startTime)
		beego.Info(fmt.Sprintf("[%s] done, cost %g s", taskName, dur.Seconds()))
		return fnErr
	}
}

func fetchData(pi pipeInterface){
	taskName := pi.(taskInterface).GetName()
	go func(){
		defer func(){
			if err := recover(); err!=nil{
				beego.Warn(string(debug.Stack()))
				fetchData(pi)
				dingMsg := fmt.Sprintf("> goroutine from task(%s) dead \n\n 错误信息: %s \n\n", taskName, err.(error).Error())
				vanilla.NewDingBot().Use("xiuer").Error(dingMsg)
			}
		}()
		for{
			data := pi.GetData()
			if data != nil{
				taskCtx := newTaskCtx()
				beego.Info(fmt.Sprintf("[%s] consume data...", taskName))
				startTime := time.Now()
				pi.RunConsumer(data, taskCtx)
				dur := time.Since(startTime)
				beego.Info(fmt.Sprintf("[%s] consume done, cost %g s !", taskName, dur.Seconds()))
			}
		}
	}()
}

func RegisterPipeTask(pi pipeInterface, spec string) *CronTask{
	task := RegisterTask(pi.(taskInterface), spec)
	if task != nil{
		if pi.EnableParallel(){ // 并行模式下，开启通道容量十分之一的goroutine消费通道
			for i := pi.GetConsumerCount(); i>0; i--{
				fetchData(pi)
			}
		}else{
			fetchData(pi)
		}
	}
	return task
}

func RegisterTask(task taskInterface, spec string) *CronTask {
	if beego.AppConfig.DefaultBool("system::ENABLE_CRON_MODE", false) || beego.AppConfig.String("system::SERVICE_MODE") == "cron" {
		tname := task.GetName()
		wrappedFn := taskWrapper(task)
		cronTask := &CronTask{
			name: tname,
			spec: spec,
			taskFunc: wrappedFn,
			onlyRunThisTask: false,
		}
		name2task[tname] = cronTask
		
		return cronTask
	} else {
		return nil
	}
}

func RegisterTaskInRestMode(task taskInterface, spec string) *CronTask {
	if !beego.AppConfig.DefaultBool("system::ENABLE_CRON_MODE", false) && beego.AppConfig.String("system::SERVICE_MODE") == "rest" {
		tname := task.GetName()
		wrappedFn := taskWrapper(task)
		cronTask := &CronTask{
			name: tname,
			spec: spec,
			taskFunc: wrappedFn,
			onlyRunThisTask: false,
		}
		name2task[tname] = cronTask
		
		return cronTask
	} else {
		return nil
	}
}

func RegisterCronTask(tname string, spec string, f toolbox.TaskFunc) *CronTask {
	cronTask := &CronTask{
		name: tname,
		spec: spec,
		taskFunc: f,
		onlyRunThisTask: false,
	}
	name2task[tname] = cronTask

	return cronTask
}

func StartCronTasks() {
	var onlyRunTask *CronTask
	for _, task := range name2task {
		if task.onlyRunThisTask {
			onlyRunTask = task
		}
	}

	if onlyRunTask != nil {
		cronTask := onlyRunTask
		beego.Info("[cron] create cron task ", cronTask.name, cronTask.spec)
		task := toolbox.NewTask(cronTask.name, cronTask.spec, cronTask.taskFunc)
		toolbox.AddTask(cronTask.name, task)
	} else {
		for _, cronTask := range name2task {
			beego.Info("[cron] create cron task ", cronTask.name, cronTask.spec)
			task := toolbox.NewTask(cronTask.name, cronTask.spec, cronTask.taskFunc)
			toolbox.AddTask(cronTask.name, task)
		}
	}

	toolbox.StartTask()
}

func StopCronTasks() {
	toolbox.StopTask()
}
