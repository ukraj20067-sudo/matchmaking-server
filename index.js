const { Server } = require("socket.io");
const http = require("http");

const server = http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
        res.writeHead(200);
        res.end("OK");
    } else {
        res.writeHead(404);
        res.end();
    }
});

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

const queues = {};
const privateRooms = {}; // { roomId: { gameKey, requiredPlayers, players: [...] } }

io.on("connection", (socket) => {
    console.log(`Connected: ${socket.id}`);

    // ── Public matchmaking ──────────────────────────────────────────────────
    socket.on("joinQueue", (data) => {
        const { gameKey, playerId, playerName, avatarUrl, eloRating } = data;
        if (!queues[gameKey]) queues[gameKey] = [];
        queues[gameKey] = queues[gameKey].filter(p => p.playerId !== playerId);
        queues[gameKey].push({ socketId: socket.id, playerId, playerName, avatarUrl, eloRating: eloRating || 1000, joinedAt: Date.now(), socket });
        console.log(`${playerName} joined ${gameKey} queue`);
        tryMatchmaking(gameKey);
    });

    socket.on("cancelSearch", (data) => {
        const { gameKey, playerId } = data;
        if (queues[gameKey]) queues[gameKey] = queues[gameKey].filter(p => p.playerId !== playerId);
    });

    // ── Private rooms ───────────────────────────────────────────────────────
    socket.on("createPrivateRoom", (data) => {
        const { gameKey, playerId, playerName, avatarUrl, requiredPlayers = 2 } = data;
        const roomId = Math.random().toString(36).substring(2, 8).toUpperCase();

        socket.join(roomId);
        privateRooms[roomId] = {
            gameKey,
            requiredPlayers: Math.min(Math.max(parseInt(requiredPlayers) || 2, 2), 4),
            players: [{ socketId: socket.id, playerId, playerName, avatarUrl, colorIndex: 0 }]
        };

        socket.emit("privateRoomCreated", { roomId, requiredPlayers: privateRooms[roomId].requiredPlayers });
        console.log(`Private room ${roomId} created by ${playerName} (${requiredPlayers} players)`);
    });

    socket.on("joinPrivateRoom", (data) => {
        const { roomId, playerId, playerName, avatarUrl } = data;
        const upperRoomId = roomId.toUpperCase();
        const room = privateRooms[upperRoomId];

        if (!room) {
            socket.emit("roomError", { message: "Room not found or already started." });
            return;
        }
        if (room.players.find(p => p.socketId === socket.id)) {
            socket.emit("roomError", { message: "Already in room." });
            return;
        }
        if (room.players.length >= room.requiredPlayers) {
            socket.emit("roomError", { message: "Room is full." });
            return;
        }

        const colorIndex = room.players.length;
        room.players.push({ socketId: socket.id, playerId, playerName, avatarUrl, colorIndex });
        socket.join(upperRoomId);

        const status = {
            roomId: upperRoomId,
            currentCount: room.players.length,
            requiredPlayers: room.requiredPlayers,
            playerNames: room.players.map(p => p.playerName)
        };

        // Notify everyone in room of updated status
        io.to(upperRoomId).emit("roomStatus", status);
        console.log(`${playerName} joined room ${upperRoomId} (${room.players.length}/${room.requiredPlayers})`);

        // Start game when room is full
        if (room.players.length === room.requiredPlayers) {
            const gameStartData = {
                roomId: upperRoomId,
                players: room.players.map(p => ({
                    socketId: p.socketId,
                    playerId: p.playerId,
                    playerName: p.playerName,
                    colorIndex: p.colorIndex
                }))
            };
            setTimeout(() => {
                io.to(upperRoomId).emit("gameStart", gameStartData);
                console.log(`Game starting in room ${upperRoomId}`);
                delete privateRooms[upperRoomId];
            }, 500);
        }
    });

    // ── Socket.io game move relay (for Ludo multi-player) ──────────────────
    socket.on("game_event", (data) => {
        // Relay to everyone else in the room
        socket.to(data.roomId).emit("game_event", data);
    });

    // ── WebRTC signaling (for Chess 1v1) ───────────────────────────────────
    socket.on("joinRoom", (roomId) => {
        socket.join(roomId);
    });

    socket.on("webrtc_offer", (data) => {
        socket.to(data.roomId).emit("webrtc_offer", data.sdp);
    });

    socket.on("webrtc_answer", (data) => {
        socket.to(data.roomId).emit("webrtc_answer", data.sdp);
    });

    socket.on("webrtc_ice_candidate", (data) => {
        socket.to(data.roomId).emit("webrtc_ice_candidate", data.candidate);
    });

    // ── Disconnect ──────────────────────────────────────────────────────────
    socket.on("disconnect", () => {
        console.log(`Disconnected: ${socket.id}`);
        for (const gameKey in queues) {
            queues[gameKey] = queues[gameKey].filter(p => p.socketId !== socket.id);
        }
        // Remove from waiting private rooms
        for (const roomId in privateRooms) {
            const room = privateRooms[roomId];
            const before = room.players.length;
            room.players = room.players.filter(p => p.socketId !== socket.id);
            if (room.players.length < before) {
                if (room.players.length === 0) {
                    delete privateRooms[roomId];
                } else {
                    // Re-index colorIndex
                    room.players.forEach((p, i) => p.colorIndex = i);
                    const status = {
                        roomId,
                        currentCount: room.players.length,
                        requiredPlayers: room.requiredPlayers,
                        playerNames: room.players.map(p => p.playerName)
                    };
                    io.to(roomId).emit("roomStatus", status);
                }
            }
        }
    });
});

setInterval(() => {
    for (const gameKey in queues) tryMatchmaking(gameKey);
}, 3000);

function tryMatchmaking(gameKey) {
    const queue = queues[gameKey];
    if (!queue || queue.length < 2) return;
    queue.sort((a, b) => a.joinedAt - b.joinedAt);
    let i = 0;
    while (i < queue.length) {
        const p1 = queue[i];
        const waitTime = Date.now() - p1.joinedAt;
        let eloRange = 200;
        if (waitTime > 60000) eloRange = 10000;
        else if (waitTime > 30000) eloRange = 500;
        let matchedIndex = -1;
        for (let j = i + 1; j < queue.length; j++) {
            if (Math.abs(p1.eloRating - queue[j].eloRating) <= eloRange) {
                matchedIndex = j;
                break;
            }
        }
        if (matchedIndex !== -1) {
            const p2 = queue[matchedIndex];
            const roomId = `room_${Date.now()}_${Math.random().toString(36).substring(7)}`;
            p1.socket.emit("matchFound", { roomId, isInitiator: true, opponent: { playerId: p2.playerId, playerName: p2.playerName, avatarUrl: p2.avatarUrl, eloRating: p2.eloRating } });
            p2.socket.emit("matchFound", { roomId, isInitiator: false, opponent: { playerId: p1.playerId, playerName: p1.playerName, avatarUrl: p1.avatarUrl, eloRating: p1.eloRating } });
            console.log(`Matched ${p1.playerName} vs ${p2.playerName} in ${roomId}`);
            queue.splice(matchedIndex, 1);
            queue.splice(i, 1);
        } else {
            i++;
        }
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server on port ${PORT}`));
