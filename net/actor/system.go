package cherryActor

import (
	"strings"
	"sync"
	"time"

	ccode "github.com/cherry-game/cherry/code"
	cutils "github.com/cherry-game/cherry/extend/utils"
	cfacade "github.com/cherry-game/cherry/facade"
	clog "github.com/cherry-game/cherry/logger"
	cproto "github.com/cherry-game/cherry/net/proto"
)

type (
	// System Actor系统
	System struct {
		app              cfacade.IApplication
		actorMap         *sync.Map          // key:actorID, value:*actor
		localInvokeFunc  cfacade.InvokeFunc // default local func
		remoteInvokeFunc cfacade.InvokeFunc // default remote func
		wg               *sync.WaitGroup    // wait group
		callTimeout      time.Duration      // call调用超时
		arrivalTimeOut   int64              // message到达超时(毫秒)
		executionTimeout int64              // 消息执行超时(毫秒)
	}
)

func NewSystem() *System {
	system := &System{
		actorMap:         &sync.Map{},
		localInvokeFunc:  InvokeLocalFunc,
		remoteInvokeFunc: InvokeRemoteFunc,
		wg:               &sync.WaitGroup{},
		callTimeout:      3 * time.Second,
		arrivalTimeOut:   100,
		executionTimeout: 100,
	}

	return system
}

// 设置APP的信息
func (p *System) SetApp(app cfacade.IApplication) {
	p.app = app
}

// 得到节点ID
func (p *System) NodeID() string {
	if p.app == nil {
		return ""
	}

	return p.app.NodeID()
}

// 系统停止
func (p *System) Stop() {
	p.actorMap.Range(func(key, value any) bool {
		actor, ok := value.(*Actor)
		if ok {
			cutils.Try(func() {
				actor.Exit()
			}, func(err string) {
				clog.Warnf("[OnStop] - [actorID = %s, err = %s]", actor.path, err)
			})
		}
		return true
	})

	clog.Info("actor system stopping!")
	p.wg.Wait()
	clog.Info("actor system stopped!")
}

// GetIActor 根据ActorID获取IActor
func (p *System) GetIActor(id string) (cfacade.IActor, bool) {
	return p.GetActor(id)
}

// GetActor 根据ActorID获取*actor
func (p *System) GetActor(id string) (*Actor, bool) {
	actorValue, found := p.actorMap.Load(id)
	if !found {
		return nil, false
	}

	actor, found := actorValue.(*Actor)
	return actor, found
}

// 获取子Actor
func (p *System) GetChildActor(actorID, childID string) (*Actor, bool) {
	parentActor, found := p.GetActor(actorID)
	if !found {
		return nil, found
	}

	return parentActor.child.GetActor(childID)
}

// 删除Actor
func (p *System) removeActor(actorID string) {
	p.actorMap.Delete(actorID)
}

// CreateActor 创建Actor
func (p *System) CreateActor(id string, handler cfacade.IActorHandler) (cfacade.IActor, error) {
	if strings.TrimSpace(id) == "" {
		return nil, ErrActorIDIsNil
	}
	// 如果已经找到，直接返回
	if actor, found := p.GetIActor(id); found {
		return actor, nil
	}
	// 创建一个actor
	thisActor, err := newActor(id, "", handler, p)
	if err != nil {
		return nil, err
	}
	// 加入到map
	p.actorMap.Store(id, thisActor) // add to map
	// 一个go routine去跑这个actor
	go thisActor.run() // new actor is running!

	return thisActor, nil
}

// Call 发送远程消息(不回复)
func (p *System) Call(source, target, funcName string, arg any) int32 {
	// 目标Actor检验
	if target == "" {
		clog.Warnf("[Call] Target path is nil. [source = %s, target = %s, funcName = %s]",
			source,
			target,
			funcName,
		)
		return ccode.ActorPathIsNil
	}
	// 调用的函数检验
	if len(funcName) < 1 {
		clog.Warnf("[Call] FuncName error. [source = %s, target = %s, funcName = %s]",
			source,
			target,
			funcName,
		)
		return ccode.ActorFuncNameError
	}
	// 获得目标路径
	targetPath, err := cfacade.ToActorPath(target)
	if err != nil {
		clog.Warnf("[Call] Target path error. [source = %s, target = %s, funcName = %s, err = %v]",
			source,
			target,
			funcName,
			err,
		)
		return ccode.ActorConvertPathError
	}

	// 不是同一个节点
	if targetPath.NodeID != "" && targetPath.NodeID != p.NodeID() {
		// 分配一个包
		clusterPacket := cproto.GetClusterPacket()
		clusterPacket.SourcePath = source
		clusterPacket.TargetPath = target
		clusterPacket.FuncName = funcName
		// 参数序列化
		if arg != nil {
			argsBytes, err := p.app.Serializer().Marshal(arg)
			if err != nil {
				clog.Warnf("[Call] Marshal arg error. [targetPath = %s, error = %s]",
					target,
					err,
				)
				return ccode.ActorMarshalError
			}
			clusterPacket.ArgBytes = argsBytes
		}
		// 发布到目标节点上
		err = p.app.Cluster().PublishRemote(targetPath.NodeID, clusterPacket)
		if err != nil {
			clog.Warnf("[Call] Publish remote fail. [source = %s, target = %s, funcName = %s, err = %v]",
				source,
				target,
				funcName,
				err,
			)
			return ccode.ActorPublishRemoteError
		}
		// 同一个节点
	} else {

		remoteMsg := cfacade.GetMessage()
		remoteMsg.Source = source
		remoteMsg.Target = target
		remoteMsg.FuncName = funcName
		remoteMsg.Args = arg
		// 提交到远程邮箱中
		if !p.PostRemote(&remoteMsg) {
			clog.Warnf("[Call] Post remote fail. [source = %s, target = %s, funcName = %s]", source, target, funcName)
			return ccode.ActorCallFail
		}
	}

	return ccode.OK
}

// CallWait 发送远程消息(等待回复)
func (p *System) CallWait(source, target, funcName string, arg, reply any) int32 {
	// 获取源Actor路径
	sourcePath, err := cfacade.ToActorPath(source)
	if err != nil {
		clog.Warnf("[CallWait] Source path error. [source = %s, target = %s, funcName = %s, err = %v]",
			source,
			target,
			funcName,
			err,
		)
		return ccode.ActorConvertPathError
	}

	// 获取目标Actor路径
	targetPath, err := cfacade.ToActorPath(target)
	if err != nil {
		clog.Warnf("[CallWait] Target path error. [source = %s, target = %s, funcName = %s, err = %v]",
			source,
			target,
			funcName,
			err,
		)
		return ccode.ActorConvertPathError
	}

	// 同一个，不需要通过Callwait调用
	if source == target {
		clog.Warnf("[CallWait] Source path is equal target. [source = %s, target = %s, funcName = %s]",
			source,
			target,
			funcName,
		)
		return ccode.ActorSourceEqualTarget
	}
	// 函数名校验
	if len(funcName) < 1 {
		clog.Warnf("[CallWait] FuncName error. [source = %s, target = %s, funcName = %s]",
			source,
			target,
			funcName,
		)
		return ccode.ActorFuncNameError
	}

	// forward to remote actor
	// 不同的节点上的Actor
	if targetPath.NodeID != "" && targetPath.NodeID != sourcePath.NodeID {
		clusterPacket := cproto.BuildClusterPacket(source, target, funcName)

		// 序列化参数
		if arg != nil {
			argsBytes, err := p.app.Serializer().Marshal(arg)
			if err != nil {
				clog.Warnf("[CallWait] Marshal arg error. [targetPath = %s, error = %s]", target, err)
				return ccode.ActorMarshalError
			}
			clusterPacket.ArgBytes = argsBytes
		}

		// 使用Request，并且等待回应
		rspData, rspCode := p.app.Cluster().RequestRemote(targetPath.NodeID, clusterPacket, p.callTimeout)
		if ccode.IsFail(rspCode) {
			return rspCode
		}

		if reply != nil {
			if err = p.app.Serializer().Unmarshal(rspData, reply); err != nil {
				clog.Warnf("[CallWait] Marshal reply error. [targetPath = %s, error = %s]", target, err)
				return ccode.ActorMarshalError
			}
		}
		// 同一个节点
	} else {
		message := cfacade.GetMessage()
		message.Source = source
		message.Target = target
		message.FuncName = funcName
		message.Args = arg
		message.ChanResult = make(chan interface{})

		var result interface{}
		// 同一个父Actor
		if sourcePath.ActorID == targetPath.ActorID {
			// 子ActorID也相同，自己调用自己的callwait
			if sourcePath.ChildID == targetPath.ChildID {
				return ccode.ActorSourceEqualTarget
			}
			// 找到子Actor
			childActor, found := p.GetChildActor(targetPath.ActorID, targetPath.ChildID)
			if !found {
				return ccode.ActorChildIDNotFound
			}
			// 提交远程消息
			childActor.PostRemote(&message)
		} else {
			if !p.PostRemote(&message) {
				clog.Warnf("[CallWait] Post remote fail. [source = %s, target = %s, funcName = %s]", source, target, funcName)
				return ccode.ActorCallFail
			}
		}
		// 等待结果
		select {
		case result = <-message.ChanResult:
			{
				if result == nil {
					clog.Warnf("[CallWait] Response is nil. [targetPath = %s]", target)
					return ccode.ActorCallFail
				}

				rsp := result.(*cproto.Response)
				if rsp == nil {
					clog.Warnf("[CallWait] Response is nil. [targetPath = %s]", target)
					return ccode.ActorCallFail
				}
				// 调用失败
				if ccode.IsFail(rsp.Code) {
					return rsp.Code
				}

				if reply != nil {
					if rsp.Data == nil {
						clog.Warnf("[CallWait] rsp.Data is nil. [targetPath = %s, error = %s]", target, err)
					}
					// 序列化回应
					err = p.app.Serializer().Unmarshal(rsp.Data, reply)
					if err != nil {
						clog.Warnf("[CallWait] Unmarshal reply error. [targetPath = %s, error = %s]", target, err)
						return ccode.ActorUnmarshalError
					}
				}
			}
		// 超时
		case <-time.After(p.callTimeout):
			return ccode.ActorCallTimeout
		}
	}

	return ccode.OK
}

// Broadcast 根据节点类型发布消息
func (p *System) CallType(nodeType, actorID, funcName string, arg any) int32 {
	if actorID == "" {
		return ccode.ActorIDIsNil
	}

	if len(funcName) < 1 {
		clog.Warnf("[CallType] FuncName error. [nodeType = %s, actorID = %s, funcName = %s]",
			nodeType,
			actorID,
			funcName,
		)
		return ccode.ActorFuncNameError
	}

	clusterPacket := cproto.GetClusterPacket()
	clusterPacket.TargetPath = cfacade.NewPath("", actorID)
	clusterPacket.FuncName = funcName
	// 序列化参数
	if arg != nil {
		argsBytes, err := p.app.Serializer().Marshal(arg)
		if err != nil {
			clog.Warnf("[CallType] Marshal arg error. [nodeType = %s, actorID = %s, funcName = %s, error = %s]",
				nodeType,
				actorID,
				funcName,
				err,
			)
			return ccode.ActorMarshalError
		}
		clusterPacket.ArgBytes = argsBytes
	}
	// 根据节点类型发布远程消息
	err := p.app.Cluster().PublishRemoteType(nodeType, clusterPacket)
	if err != nil {
		clog.Warnf("[CallType] Publish remote fail. [nodeType = %s, actorID = %s, funcName = %s, err = %v]",
			nodeType,
			actorID,
			funcName,
			err,
		)
		return ccode.ActorPublishRemoteError
	}

	return ccode.OK
}

// PostRemote 提交远程消息
func (p *System) PostRemote(m *cfacade.Message) bool {
	if m == nil {
		clog.Error("Message is nil.")
		return false
	}
	// 先根据目标路径中的ActorID找到对应的Actor
	if targetActor, found := p.GetActor(m.TargetPath().ActorID); found {
		// 只有工作状态，才提交远程消息
		if targetActor.state == WorkerState {
			targetActor.PostRemote(m)
		}
		return true
	}

	clog.Warnf("[PostRemote] actor not found. [source = %s, target = %s -> %s]", m.Source, m.Target, m.FuncName)
	return false
}

// PostLocal 提交本地消息
func (p *System) PostLocal(m *cfacade.Message) bool {
	if m == nil {
		clog.Error("Message is nil.")
		return false
	}
	// 先根据目标路径中的ActorID找到对应的Actor
	if targetActor, found := p.GetActor(m.TargetPath().ActorID); found {
		// 只有工作状态，才提交远程消息
		if targetActor.state == WorkerState {
			targetActor.PostLocal(m)
		}
		return true
	}

	clog.Warnf("[PostLocal] actor not found. [source = %s, target = %s -> %s]", m.Source, m.Target, m.FuncName)

	return false
}

// PostEvent 提交事件，这里的事件是全局事件，注意！！！
func (p *System) PostEvent(data cfacade.IEventData) {
	if data == nil {
		clog.Error("[PostEvent] Event is nil.")
		return
	}

	// range root actor
	// 事件会抛给所有的Actor和子Actor
	p.actorMap.Range(func(key, value any) bool {
		if thisActor, found := value.(*Actor); found {
			if thisActor.state == WorkerState {
				thisActor.event.Push(data)
			}

			// range child actor
			thisActor.Child().Each(func(iActor cfacade.IActor) {
				if childActor, ok := iActor.(*Actor); ok {
					childActor.event.Push(data)
				}
			})
		}
		return true
	})
}

// 设置本地调用函数
func (p *System) SetLocalInvoke(fn cfacade.InvokeFunc) {
	if fn != nil {
		p.localInvokeFunc = fn
	}
}

// 设置远程调用函数
func (p *System) SetRemoteInvoke(fn cfacade.InvokeFunc) {
	if fn != nil {
		p.remoteInvokeFunc = fn
	}
}

// 设置调用超时
func (p *System) SetCallTimeout(d time.Duration) {
	p.callTimeout = d
}

// 设置到达超时
func (p *System) SetArrivalTimeout(t int64) {
	if t > 1 {
		p.arrivalTimeOut = t
	}
}

// 设置执行超时
func (p *System) SetExecutionTimeout(t int64) {
	if t > 1 {
		p.executionTimeout = t
	}
}
