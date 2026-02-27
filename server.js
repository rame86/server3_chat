// ✅ PostgreSQL 드라이버(pg)에서 Pool 가져오기
const { Pool } = require("pg");

// ✅ Express: HTTP 서버(정적 파일 제공, 업로드 API 등)
const express = require("express");

// ✅ Node 기본 모듈들
const http = require("http");
const path = require("path");
const fs = require("fs");

// ✅ 파일 업로드(multipart/form-data) 처리 라이브러리
const multer = require("multer");

// ✅ WebSocket 기반 실시간 통신(Socket.IO)
const { Server } = require("socket.io");


// =========================
// 1) PostgreSQL 연결 설정
// =========================
// PostgreSQL 연결 (Docker 컨테이너의 Postgres에 접속)
// - host: 로컬에서 Docker 포트포워딩으로 5432 접근
// - user/password/database: docker-compose.yml에서 설정한 값
const pool = new Pool({
  host: "localhost",
  port: 5432,
  user: "chatuser",
  password: "chatpass",
  database: "chatdb",
});


// =========================
// 2) Express/HTTP/Socket.IO 기본 세팅
// =========================
const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*" } // 로컬 테스트용
});


// =========================
// 3) 정적 파일 제공(public, uploads)
// =========================
// public 폴더의 파일을 웹으로 제공 (index.html 등)
app.use(express.static("public"));

// uploads 폴더가 없으면 자동 생성(업로드 에러 방지)
const uploadDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// 업로드된 파일을 URL로 접근 가능하게 제공
// 예: http://localhost:3003/uploads/파일명.png
app.use("/uploads", express.static(uploadDir));


// =========================
// 4) multer 업로드 설정
// =========================
// multer 설정: uploads 폴더에 저장, 파일명 중복 방지
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) => {
    const safeOriginal = file.originalname.replace(/\s+/g, "_");
    cb(null, `${Date.now()}-${Math.random().toString(16).slice(2)}-${safeOriginal}`);
  },
});

// 이미지 파일만 허용(간단 필터)
function imageOnly(req, file, cb) {
  if (file.mimetype && file.mimetype.startsWith("image/")) return cb(null, true);
  cb(new Error("Only image files are allowed"));
}

const upload = multer({
  storage,
  fileFilter: imageOnly,
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB 제한(데모용)
});


// =========================
// 5) HTTP API 라우트들
// =========================
// 동작 확인용(팀장에게 보여주기 좋음)
app.get("/health", (req, res) => {
  res.json({ ok: true, service: "chat-demo", time: new Date().toISOString() });
});

// 이미지 업로드 API (multipart/form-data)
// 프론트에서 form.append("image", file)로 보내는 필드명이 "image"여야 함
app.post("/upload", upload.single("image"), (req, res) => {
  // 업로드 성공 시 req.file 존재
  if (!req.file) {
    return res.status(400).json({ ok: false, message: "No file uploaded" });
  }

  // 브라우저에서 접근 가능한 URL로 반환
  const fileUrl = `/uploads/${req.file.filename}`;
  res.json({ ok: true, url: fileUrl });
});


// =========================
// 6) Socket.IO 이벤트 처리 (채팅 핵심)
// =========================
io.on("connection", (socket) => {
  console.log("connected:", socket.id);

  // -------------------------
  // (A) 방 입장(join)
  // -------------------------
  // join 시점에 할 일:
  // 1) socket.join(roomId)로 방 참여
  // 2) DB에서 최근 메시지 50개 조회해서 "이 소켓에게만" 전달
  // 3) 시스템 메시지(입장) 브로드캐스트
  socket.on("join", async ({ roomId, userId, role }) => {
    socket.join(roomId);
    socket.data = { roomId, userId, role };

    try {
      // 최근 50개 조회 (최신순으로 가져온 뒤 reverse 해서 시간순으로 보냄)
      const result = await pool.query(
        `SELECT id, room_id, user_id, role, type, content, created_at
         FROM messages
         WHERE room_id = $1
         ORDER BY created_at DESC
         LIMIT 50`,
        [roomId]
      );

      const messages = result.rows.reverse();

      // "이 방에 들어온 사람(socket)"에게만 과거 메시지 전송
      for (const msg of messages) {
        socket.emit("message", {
          roomId: msg.room_id,
          userId: msg.user_id,
          role: msg.role,
          type: msg.type,
          content: msg.content,
          at: msg.created_at, // DB 시간
        });
      }
    } catch (err) {
      console.error("DB select error:", err);
    }

    // 방 전체에게 입장 시스템 메시지 알림
    io.to(roomId).emit("system", {
      message: `${role}(${userId}) joined room ${roomId}`,
      at: new Date().toISOString(),
    });
  });

  // -------------------------
  // (B) 메시지 전송(message)
  // -------------------------
  // message 시점에 할 일:
  // 1) DB에 저장(INSERT)
  // 2) 저장 성공 후 방 전체에게 브로드캐스트
  socket.on("message", async ({ roomId, userId, role, type, content }) => {
    try {
      // 1) DB 저장
      await pool.query(
        `INSERT INTO messages (room_id, user_id, role, type, content)
         VALUES ($1, $2, $3, $4, $5)`,
        [roomId, userId, role, type, content]
      );

      // 2) 실시간 전송 (방 전체)
      io.to(roomId).emit("message", {
        roomId,
        userId,
        role,
        type,      // TEXT | IMAGE
        content,
        at: new Date().toISOString(),
      });
    } catch (err) {
      console.error("DB insert error:", err);
    }
  });

  // -------------------------
  // (C) 연결 종료(disconnect)
  // -------------------------
  socket.on("disconnect", () => {
    const { roomId, userId, role } = socket.data || {};
    if (roomId) {
      io.to(roomId).emit("system", {
        message: `${role}(${userId}) left room ${roomId}`,
        at: new Date().toISOString(),
      });
    }
    console.log("disconnected:", socket.id);
  });
});


// =========================
// 7) 서버 실행
// =========================
const PORT = 3003;
server.listen(PORT, () => {
  console.log(`✅ chat demo running: http://localhost:${PORT}`);
});