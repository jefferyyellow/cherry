package cherryActor

import cfacade "github.com/cherry-game/cherry/facade"

var (
	Name = "actor_component"
)

// 组件
type Component struct {
	cfacade.Component
	*System
	// 注册到组件的Actor
	actorHandlers []cfacade.IActorHandler
}

// 创建一个新组件
func New() *Component {
	return &Component{
		System: NewSystem(),
	}
}

// 组件的名字
func (c *Component) Name() string {
	return Name
}

// 初始化组件
func (c *Component) Init() {
	c.System.SetApp(c.App())
}

// 初始化后调用
func (c *Component) OnAfterInit() {
	// Register actor
	// 创建Actor
	for _, actor := range c.actorHandlers {
		c.CreateActor(actor.AliasID(), actor)
	}
}

// 停止前调用
func (c *Component) OnStop() {
	c.System.Stop()
}

// 增加Actor
func (c *Component) Add(actors ...cfacade.IActorHandler) {
	c.actorHandlers = append(c.actorHandlers, actors...)
}
