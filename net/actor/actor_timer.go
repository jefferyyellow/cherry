package cherryActor

import (
	"time"

	cherryTimeWheel "github.com/cherry-game/cherry/extend/time_wheel"
	cutils "github.com/cherry-game/cherry/extend/utils"
	clog "github.com/cherry-game/cherry/logger"
)

const (
	updateTimerFuncName = "_updateTimer_"
)

// Actor的定时器系统
type (
	actorTimer struct {
		thisActor    *Actor
		timerInfoMap map[uint64]*timerInfo //key:timerID,value:*timerInfo
	}
	// 定时器信息
	timerInfo struct {
		// 时间轮定时器
		timer *cherryTimeWheel.Timer
		// 回调函数
		fn func()
		// 是否会一次性定时器
		once bool
	}
)

// 创建定时器系统
func newTimer(thisActor *Actor) actorTimer {
	return actorTimer{
		thisActor:    thisActor,
		timerInfoMap: make(map[uint64]*timerInfo),
	}
}

// 停止前调用
func (p *actorTimer) onStop() {
	p.RemoveAll()
	p.thisActor = nil
}

// 新增一个定时器
func (p *actorTimer) Add(delay time.Duration, fn func(), async ...bool) uint64 {
	// 小于1毫秒，或者回调为nil
	if delay.Milliseconds() < 1 || fn == nil {
		clog.Warnf("[ActorTimer] Add parameter error. delay = %+v", delay)
		return 0
	}
	// 分配一个ID
	newID := globalTimer.NextID()
	timer := globalTimer.AddEveryFunc(newID, delay, p.callUpdateTimer(newID), async...)

	if timer == nil {
		clog.Warnf("[ActorTimer] Add error. delay = %+v", delay)
		return 0
	}
	// 添加回调信息
	p.addTimerInfo(timer, fn, false)

	return newID
}

// 增加一个一次性的定时器
func (p *actorTimer) AddOnce(delay time.Duration, fn func(), async ...bool) uint64 {
	if delay.Milliseconds() < 1 || fn == nil {
		clog.Warnf("[ActorTimer] AddOnce parameter error. delay = %+v", delay)
		return 0
	}
	// 分配一个ID
	newID := globalTimer.NextID()
	// 时间到了，调更新函数
	timer := globalTimer.AfterFunc(newID, delay, p.callUpdateTimer(newID), async...)

	if timer == nil {
		clog.Warnf("[ActorTimer] AddOnce error. d = %+v", delay)
		return 0
	}

	p.addTimerInfo(timer, fn, true)

	return newID
}

// 增加一个确定时间点的定时器，指定：时：分：秒
func (p *actorTimer) AddFixedHour(hour, minute, second int, fn func(), async ...bool) uint64 {
	schedule := &cherryTimeWheel.FixedDateSchedule{
		Hour:   hour,
		Minute: minute,
		Second: second,
	}

	return p.AddSchedule(schedule, fn, async...)
}

// 增加一个确定时间点的定时器，指定：-1：分：秒
func (p *actorTimer) AddFixedMinute(minute, second int, fn func(), async ...bool) uint64 {
	return p.AddFixedHour(-1, minute, second, fn, async...)
}

// 增加一个计划
func (p *actorTimer) AddSchedule(s ITimerSchedule, fn func(), async ...bool) uint64 {
	if s == nil || fn == nil {
		return 0
	}

	newID := globalTimer.NextID()
	timer := globalTimer.ScheduleFunc(newID, s, p.callUpdateTimer(newID), async...)

	p.addTimerInfo(timer, fn, false)

	return newID
}

// 删除定时器
func (p *actorTimer) Remove(id uint64) {
	funcItem, found := p.timerInfoMap[id]
	if found {
		// 停止定时器
		funcItem.timer.Stop()
		// 删除定时器相关信息
		delete(p.timerInfoMap, id)
	}
}

// 删除所有定时器
func (p *actorTimer) RemoveAll() {
	for _, info := range p.timerInfoMap {
		info.timer.Stop()
	}
}

// 增加定时器
func (p *actorTimer) addTimerInfo(timer *cherryTimeWheel.Timer, fn func(), once bool) {
	p.timerInfoMap[timer.ID()] = &timerInfo{
		timer: timer,
		fn:    fn,
		once:  once,
	}
}

// 调用更新函数
func (p *actorTimer) callUpdateTimer(id uint64) func() {
	return func() {
		p.thisActor.Call(p.thisActor.PathString(), updateTimerFuncName, id)
	}
}

// 更新函数
func (p *actorTimer) _updateTimer_(id uint64) {
	// 根据计时器ID得到对应的计时器数据
	value, found := p.timerInfoMap[id]
	if !found {
		return
	}

	cutils.Try(func() {
		// 调用回调
		value.fn()
	}, func(errString string) {
		clog.Error(errString)
	})
	// 如果是一次性的计时器，删除计时器相关数据
	if value.once {
		delete(p.timerInfoMap, id)
	}
}
