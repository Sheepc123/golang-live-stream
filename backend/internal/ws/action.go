package ws

import "log"

// action dispatch different type msg
type Action interface {

	// require a Client paramater. provide session context(e.g., userId,roomId)
	// trust Client context instead of msg context which from frontend.
	Execute(client *Client, msg Message)
}

type ActionRegistry struct {
	action map[string]Action
}

func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{
		action: make(map[string]Action),
	}
}

// Register binds a specific message type to its corresponding handler
func (r *ActionRegistry) Register(msgType string, action Action) {
	r.action[msgType] = action
}

func (r *ActionRegistry) Dispatch(client *Client, msg Message) {
	action,ok := r.action[msg.Type]

	if !ok {
		log.Printf("Error, no action registered for this message type : %v", msg.Type)
		return
	}

	action.Execute(client,msg)
}

