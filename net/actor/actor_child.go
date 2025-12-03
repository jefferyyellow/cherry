package cherryActor

import (
	"strings"
	"sync"

	cherryCode "github.com/cherry-game/cherry/code"
	cfacade "github.com/cherry-game/cherry/facade"
)

// 子Actor管理器
type actorChild struct {
	thisActor   *Actor
	childActors *sync.Map // key:childActorID, value:*actor
}

func newChild(thisActor *Actor) actorChild {
	return actorChild{
		thisActor:   thisActor,
		childActors: &sync.Map{},
	}
}

// 停止，通知所有的子Actor退出
func (p *actorChild) onStop() {
	p.childActors.Range(func(key, value any) bool {
		if childActor, ok := value.(*Actor); ok {
			childActor.Exit()
		}
		return true
	})

	//p.childActors = nil
	p.thisActor = nil
}

// 创建子Actor
func (p *actorChild) Create(childID string, handler cfacade.IActorHandler) (cfacade.IActor, error) {
	// 自己就是子Actor，子Actor不能嵌套子Actor
	if p.thisActor.path.IsChild() {
		return nil, ErrForbiddenCreateChildActor
	}

	if strings.TrimSpace(childID) == "" {
		return nil, ErrActorIDIsNil
	}
	// 这个子ID已经有了，直接返回子Actor
	if thisActor, ok := p.Get(childID); ok {
		return thisActor, nil
	}
	// 创建一个新的Actor
	childActor, err := newActor(p.thisActor.ActorID(), childID, handler, p.thisActor.system)
	if err != nil {
		return nil, err
	}
	// 加入到map中
	p.childActors.Store(childID, childActor)
	// 开始run
	go childActor.run()

	return childActor, nil
}

// 通过子actorID，得到actor
func (p *actorChild) Get(childID string) (cfacade.IActor, bool) {
	return p.GetActor(childID)
}

func (p *actorChild) GetActor(childID string) (*Actor, bool) {
	if actorValue, ok := p.childActors.Load(childID); ok {
		actor, found := actorValue.(*Actor)
		return actor, found
	}

	return nil, false
}

// 删除子Actor
func (p *actorChild) Remove(childID string) {
	p.childActors.Delete(childID)
}

// 遍历执行指定函数
func (p *actorChild) Each(fn func(cfacade.IActor)) {
	p.childActors.Range(func(key, value any) bool {
		if actor, found := value.(*Actor); found {
			fn(actor)
		}
		return true
	})
}

// 调用某个子actor的函数
func (p *actorChild) Call(childID, funcName string, args any) {
	if childActor, found := p.Get(childID); found {
		path := cfacade.NewChildPath("", p.thisActor.ActorID(), childID)
		childActor.Call(path, funcName, args)
	}
}

// 调用某个子actor的函数并等待返回
func (p *actorChild) CallWait(childID, funcName string, arg, reply any) int32 {
	if childActor, found := p.Get(childID); found {
		path := cfacade.NewChildPath("", p.thisActor.ActorID(), childID)
		return childActor.CallWait(path, funcName, arg, reply)
	}

	return cherryCode.ActorCallFail
}
