package ws

type LikeAction struct {
	manager     *Manager
	likecounter *LikeCounter
}

func NewLikeAction(m *Manager, c *LikeCounter) *LikeAction {
	return &LikeAction{
		manager:     m,
		likecounter: c,
	}
}

func (a *LikeAction) Execute(c *Client, m Message) {

	// Increment the number of like and return the total number 
	total := a.likecounter.Incr(c.RoomID)
	LikeCountmsg := NewLikeMessageCount(c.RoomID, total)
	a.manager.BroadcastToRoom(c.RoomID, LikeCountmsg)

}
