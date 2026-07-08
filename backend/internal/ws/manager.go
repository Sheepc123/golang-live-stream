package ws

import (
	"log"
	"sync"
)

// Manager handle the all websockets connection.
// It is responsble for :
// 1. tracking which clients are connected to each room
// 2. Registering and unregistering clients
// 3. Broadcasting the message to the client s in the same room

type Manager struct {

	// // rooms[1] = {
	// 	ClientA : true,
	// 	ClientB : true,
	// }
	// map[*Client]bool is used as a set
	rooms map[int64]map[*Client]bool

	// mu protect rooms
	// websocket concurrency situation:
	// User A  join the room
	// User B  leave the room
	// User C  send the content
	mu sync.RWMutex
}

// NewManager create new websocket Manager
func NewManager() *Manager {
	return &Manager{
		rooms: make(map[int64]map[*Client]bool),
	}
}

// Register add a new client to the corresponding live stream room.
func (m *Manager) Register(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.rooms[client.RoomID]

	if !ok {
		m.rooms[client.RoomID] = make(map[*Client]bool)
	}
	m.rooms[client.RoomID][client] = true
}

// Unregister remove a client to the corresponding live stream room
func (m *Manager) Unregister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	clients, ok := m.rooms[client.RoomID]

	if !ok {
		return
	}

	if _, ok := clients[client]; ok {
		delete(clients, client)
		close(client.Send)
	}

	if len(clients) == 0 {
		delete(m.rooms, client.RoomID)
	}
}

// BroadcastToRoom  broadcasts a message to all  clients in a specific live stream room
func (m *Manager) BroadcastToRoom(roomId int64, msg Message) {

	m.mu.RLock()
	defer m.mu.RUnlock()

	clients, ok := m.rooms[roomId]

	if !ok {
		return
	}

	for client := range clients {
		select {
		case client.Send <- msg:
		default:
			log.Printf("Client %v is too slow, drop message in room %v", client.UserID, client.RoomID)
		}
	}
}

// OnlineCount return the number of online users in a 	specfic live stream room
func (m *Manager) OnlineCount(roomId int64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clients, ok := m.rooms[roomId]

	if !ok {
		return 0
	}

	return len(clients)
}
