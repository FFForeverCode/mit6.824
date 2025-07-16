package mr

import (
	"strconv"
	"time"
)

/*
*
任务类型，包含一个任务的主要信息
*/
type Task struct {
	Status        byte
	IsMap         bool
	CreateTime    time.Time
	UpdateTime    time.Time
	MapId         int
	ReduceId      int
	ReduceNum     int
	InputFilePath string
	Coordinator   *Coordinator
}

func (task *Task) GetIntermediateOutputFilePath(reduceId int) string {
	if !task.IsMap {
		panic("not a map task")
	}
	return "result/intermediate/mr-" + strconv.Itoa(task.MapId) + "-" + strconv.Itoa(reduceId)
}

func (task *Task) GetReduceOutputFilePath() string {
	if task.IsMap {
		panic("not a reduce task")
	}
	return "result/output/mr-out-" + strconv.Itoa(task.ReduceId)
}
