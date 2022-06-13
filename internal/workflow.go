package internal

type WorkflowModel struct {
	Id        int64
	CreatedAt int64       `json:"created_at"` // 创建时间戳
	Status    TimerStatus `json:"status"`
}

type TaskModel struct {
	Id         int64
	WorkflowId int64
	CreatedAt  int64       `json:"created_at"` // 创建时间戳
	Status     TimerStatus `json:"status"`
}

