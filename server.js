// =========================
// 0) .env 환경변수 로드
// =========================
process.env.TZ = "Asia/Seoul"; // ⭐ 핵심 (반드시 최상단)
require("dotenv").config();


// =========================
// 1) 모듈 로드
// =========================
const { Pool } = require("pg");
const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const cors = require("cors");
const { Server } = require("socket.io");


// =========================
// 2) PostgreSQL 연결
// =========================
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});


// =========================
// 3) Express / HTTP / Socket.IO
// =========================
const app = express();
const server = http.createServer(app);


// CORS
app.use(cors({
  origin: "http://localhost:5173",
  methods: ["GET", "POST"],
  credentials: true
}));

app.use(express.json());


// Socket.IO
const io = new Server(server, {
  cors: {
    origin: "http://localhost:5173",
    methods: ["GET", "POST"]
  }
});


// =========================
// 4) 정적 파일
// =========================
app.use(express.static("public"));

const uploadDir = path.join(__dirname, "uploads");

if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

app.use("/uploads", express.static(uploadDir));


// =========================
// 5) multer 설정
// =========================
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const safeOriginal = file.originalname.replace(/\s+/g, "_");
    cb(null, `${Date.now()}-${Math.random().toString(16).slice(2)}-${safeOriginal}`);
  },
});

function imageOnly(req, file, cb) {
  if (file.mimetype && file.mimetype.startsWith("image/")) return cb(null, true);
  cb(new Error("Only image files are allowed"));
}

const upload = multer({
  storage,
  fileFilter: imageOnly,
  limits: { fileSize: 5 * 1024 * 1024 },
});


// =========================
// ⭐ 한국시간 함수 (간단하게)
// =========================
function getKST() {
  return new Date(); // ⭐ TZ 설정 덕분에 이미 KST
}


// =========================
// 6) HTTP API
// =========================

// health
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "chat-demo",
    time: getKST()
  });
});

// upload
app.post("/upload", upload.single("image"), (req, res) => {

  if (!req.file) {
    return res.status(400).json({
      ok: false,
      message: "No file uploaded"
    });
  }

  const fileUrl = `/uploads/${req.file.filename}`;

  res.json({ ok: true, url: fileUrl
  });

});



// 채팅방 생성
app.post("/chat/room", async (req, res) => {

  const { room_type } = req.body;

  try {

    const result = await pool.query(
      `INSERT INTO chat.chat_room (room_type)
       VALUES ($1)
       RETURNING room_id`,
      [room_type]
    );

    res.json({
      roomId: result.rows[0].room_id
    });

  } catch (err) {

    console.error("room create error:", err);

    res.status(500).json({
      error: "room create failed"
    });

  }

});


// =========================
// ⭐ 채팅방 목록 API
// =========================
app.get("/chat/rooms", async (req, res) => {
  const { userId } = req.query;

  try {

    const result = await pool.query(`
      SELECT DISTINCT ON (r.room_id)
        r.room_id,
        m.message AS last_message,
        m.created_at AS last_time
      FROM chat.chat_room r
      JOIN chat.chat_user u ON r.room_id = u.room_id
      LEFT JOIN chat.chat_message m ON r.room_id = m.room_id
      WHERE u.user_id = $1
      ORDER BY r.room_id, m.created_at DESC
    `, [userId]);

    res.json(result.rows);

  } catch (err) {
    console.error("❌ chat rooms error:", err);
    res.status(500).json({ error: "DB error" });
  }
});


// =========================
// 7) Socket.IO
// =========================
io.on("connection", (socket) => {

  console.log("connected:", socket.id);

  socket.on("join", async ({ roomId, userId, role }) => {

    socket.join(roomId);
    socket.data = { roomId, userId, role };

    try {

      const result = await pool.query(
        `SELECT message_id, room_id, sender_id, message, created_at
         FROM chat.chat_message
         WHERE room_id = $1
         ORDER BY created_at DESC
         LIMIT 50`,
        [roomId]
      );

      const messages = result.rows.reverse();

      for (const msg of messages) {

        socket.emit("message", {
          roomId: msg.room_id,
          userId: msg.sender_id,
          role: role,
          type: "text",
          content: msg.message,
          at: msg.created_at,
        });

      }

    } catch (err) {

      console.error("DB select error:", err);

    }

    io.to(roomId).emit("system", {
      message: `${role}(${userId}) joined room ${roomId}`,
      at: getKST()
    });

  });


  socket.on("message", async ({ roomId, userId, role, type, content }) => {

    try {

      await pool.query(
        `INSERT INTO chat.chat_message (room_id, sender_id, message)
         VALUES ($1, $2, $3)`,
        [roomId, userId, content]
      );

      io.to(roomId).emit("message", {
        roomId,
        userId,
        role,
        type,
        content,
        at: getKST(), // ⭐ 여기만 핵심 수정
      });

    } catch (err) {

      console.error("DB insert error:", err);

    }

  });


  socket.on("disconnect", () => {

    const { roomId, userId, role } = socket.data || {};

    if (roomId) {

      io.to(roomId).emit("system", {
        message: `${role}(${userId}) left room ${roomId}`,
        at: getKST()
      });

    }

    console.log("disconnected:", socket.id);

  });

});


// =========================
// 8) 서버 실행
// =========================
const PORT = 3003;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`chat server running on port ${PORT}`);
});

