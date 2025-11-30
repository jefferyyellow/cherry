package cherryActor

import (
	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
)

/*
* actor的事件处理系统
 */

type (
	actorEvent struct {
		thisActor *Actor                    //
		queue                               // queue,事件队列
		funcMap   map[string]*eventFuncList // register event func list 注册的函数列表
	}

	// 注册的函数列表
	eventFuncList struct {
		list     []IEventFunc
		isUnique bool
		uniqueID int64
	}
)

// 创建一个新的事件系统
func newEvent(thisActor *Actor) actorEvent {
	return actorEvent{
		thisActor: thisActor,
		queue:     newQueue(),
		funcMap:   make(map[string]*eventFuncList),
	}
}

// Register 注册事件
// name     事件名
// fn       接收事件处理的函数
// uniqueID match IEventData.UniqueID()
func (p *actorEvent) Register(name string, fn IEventFunc, uniqueID ...int64) {
	// 根据事件名称查找
	funcList, found := p.funcMap[name]
	if !found {
		// 创建新的事件名和处理列表
		funcList = &eventFuncList{}
		p.funcMap[name] = funcList
	}
	// 加入到事件响应列表中
	funcList.list = append(funcList.list, fn)

	// If a unique ID is set, it will be matched when receiving events
	// 如果设置了唯一ID，则在接收事件时将进行匹配
	funcList.isUnique = len(uniqueID) > 0
	if funcList.isUnique {
		funcList.uniqueID = uniqueID[0]
	}
}

// 注册多个事件，同一个处理函数，使用
func (p *actorEvent) Registers(names []string, fn IEventFunc, uniqueID ...int64) {
	for _, name := range names {
		p.Register(name, fn, uniqueID...)
	}
}

// Unregister 注销事件
// name 事件名
func (p *actorEvent) Unregister(name string) {
	delete(p.funcMap, name)
}

// 压入事件数据
func (p *actorEvent) Push(data cfacade.IEventData) {
	if funcList, found := p.funcMap[data.Name()]; found {
		// 查找事件是否有ID，如果有ID，只有匹配才能用
		if funcList.isUnique {
			if funcList.uniqueID == data.UniqueID() {
				p.queue.Push(data)
			}
		} else {
			p.queue.Push(data)
		}
	}
}

// 删除队列的队首，并且返回数据
func (p *actorEvent) Pop() cfacade.IEventData {
	v := p.queue.Pop()
	if v == nil {
		return nil
	}

	eventData, ok := v.(cfacade.IEventData)
	if !ok {
		clog.Warnf("Convert to IEventData fail. v = %+v", v)
		return nil
	}

	return eventData
}

// 调用事件的处理函数
func (p *actorEvent) invokeFunc(data cfacade.IEventData) {
	funcList, found := p.funcMap[data.Name()]
	// 没有找到，打印警告信息
	if !found {
		clog.Warnf("[%s] Event not found. [data = %+v]",
			p.thisActor.Path(),
			data,
		)
		return
	}

	// 定义恢复函数，避免某一个事件处理出现了panic，弄崩整个系统
	defer func() {
		if rev := recover(); rev != nil {
			clog.Errorf("[%s] Event invoke error. [data = %+v]",
				p.thisActor.Path(),
				data,
			)
		}
	}()

	for _, eventFunc := range funcList.list {
		eventFunc(data)
	}
}

func (p *actorEvent) onStop() {
	p.funcMap = nil
	p.queue.Destroy()
	p.thisActor = nil
}
