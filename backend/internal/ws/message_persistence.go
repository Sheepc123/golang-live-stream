package ws

import (
	"context"
	"log"
	"time"

	"github.com/Sheepc123/golang-live-stream/internal/service"
)

func saveMessageAsynv(msgService *service.MsgService, msg Message) {
	if msgService == nil {
		log.Printf("skip saving websocket message : message service is nil, type = %q", msg.Type)
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		err := msgService.SaveMsg(ctx, msg.RoomID, msg.UserID, msg.Username, msg.Content, msg.Type)

		if err != nil {
			log.Printf("fail to save message to db (room=%d, user=%d): %v",
				msg.RoomID, msg.UserID, err)
		}
	}()
}
