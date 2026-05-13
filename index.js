// --- FILE: matchmaking-server/index.js ---
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
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Matchmaking queues per game key
const queues = {};

io.on("connection", (socket) => {
    console.log(`Client connected: ${socket.id}`);

    socket.on("joinQueue", (data) => {
        const { gameKey, playerId, playerName, avatarUrl, eloRating, requiredPlayers } = data;
        
        if (!queues[gameKey]) {
            queues[gameKey] = [];
        }

        const playerEntry = {
            socketId: socket.id,
            playerId,
            playerName,
            avatarUrl,
            eloRating: eloRating || 1000,
            joinedAt: Date.now(),
            socket: socket
        };
        
        queues[gameKey] = queues[gameKey].filter(p => p.playerId !== playerId);
        queues[gameKey].push(playerEntry);
        
        console.log(`Player ${playerName} (${eloRating}) joined ${gameKey} queue.`);
        
        tryMatchmaking(gameKey);
    });

    socket.on("cancelSearch", (data) => {
         const { gameKey, playerId } = data;
         if (queues[gameKey]) {
             queues[gameKey] = queues[gameKey].filter(p => p.playerId !== playerId);
         }
    });

    // WebRTC Signaling Events
    socket.on("joinRoom", (roomId) => {
        socket.join(roomId);
        console.log(`Socket ${socket.id} joined room ${roomId}`);
    });

    socket.on("webrtc_offer", (data) => {
        // Forward offer to the other peer in the room
        socket.to(data.roomId).emit("webrtc_offer", data.sdp);
    });

    socket.on("webrtc_answer", (data) => {
        // Forward answer to the other peer in the room
        socket.to(data.roomId).emit("webrtc_answer", data.sdp);
    });

    socket.on("webrtc_ice_candidate", (data) => {
        // Forward ICE candidate to the other peer in the room
        socket.to(data.roomId).emit("webrtc_ice_candidate", data.candidate);
    });

    socket.on("disconnect", () => {
        console.log(`Client disconnected: ${socket.id}`);
        for (const gameKey in queues) {
            queues[gameKey] = queues[gameKey].filter(p => p.socketId !== socket.id);
        }
    });
});

setInterval(() => {
    for (const gameKey in queues) {
        tryMatchmaking(gameKey);
    }
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
            const p2 = queue[j];
            if (Math.abs(p1.eloRating - p2.eloRating) <= eloRange) {
                matchedIndex = j;
                break;
            }
        }

        if (matchedIndex !== -1) {
            const p2 = queue[matchedIndex];
            const roomId = `room_${Date.now()}_${Math.random().toString(36).substring(7)}`;
            
            // P1 is the initiator (will create the WebRTC Offer)
            p1.socket.emit("matchFound", {
                roomId: roomId,
                isInitiator: true,
                opponent: {
                    playerId: p2.playerId,
                    playerName: p2.playerName,
                    avatarUrl: p2.avatarUrl,
                    eloRating: p2.eloRating
                }
            });

            // P2 is the receiver (will wait for Offer, then create Answer)
            p2.socket.emit("matchFound", {
                roomId: roomId,
                isInitiator: false,
                opponent: {
                    playerId: p1.playerId,
                    playerName: p1.playerName,
                    avatarUrl: p1.avatarUrl,
                    eloRating: p1.eloRating
                }
            });

            console.log(`Matched ${p1.playerName} vs ${p2.playerName} in ${roomId}`);

            queue.splice(matchedIndex, 1);
            queue.splice(i, 1);
        } else {
            i++;
        }
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Matchmaking server listening on port ${PORT}`);
});
