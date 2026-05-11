import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import { createServer as createViteServer } from "vite";
import path from "path";
import multer from "multer";
import fs from "fs-extra";
import crypto from "crypto";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = 3000;
const UPLOADS_DIR = path.join(__dirname, "uploads");
const METADATA_FILE = path.join(__dirname, "metadata.json_db"); // Local "DB" for file info

// Ensure uploads directory exists
fs.ensureDirSync(UPLOADS_DIR);
if (!fs.existsSync(METADATA_FILE)) {
  fs.writeJsonSync(METADATA_FILE, { files: {}, peers: {} });
}

async function startServer() {
  const app = express();
  const httpServer = createServer(app);
  const io = new Server(httpServer, {
    cors: {
      origin: "*",
    },
  });

  app.use(express.json());

  // --- API Routes ---

  // Check if a file with a specific hash exists (Deduplication check)
  app.get("/api/files/check/:hash", (req, res) => {
    const { hash } = req.params;
    const exists = fs.existsSync(path.join(UPLOADS_DIR, hash));
    res.json({ exists });
  });

  // Get metadata for all files
  app.get("/api/files", (req, res) => {
    const db = fs.readJsonSync(METADATA_FILE);
    res.json(db.files);
  });

  // Upload file (Fallback if P2P fails or for initial seeding)
  const upload = multer({ dest: "temp_uploads/" });
  app.post("/api/upload", upload.single("file"), async (req, res) => {
    const file = req.file;
    const hash = req.body.hash;

    if (!file || !hash) {
      return res.status(400).json({ error: "Missing file or hash" });
    }

    const finalPath = path.join(UPLOADS_DIR, hash);
    
    // Deduplication check
    if (fs.existsSync(finalPath)) {
      await fs.remove(file.path);
      return res.json({ status: "exists", hash });
    }

    await fs.move(file.path, finalPath);
    
    // Update metadata
    const db = fs.readJsonSync(METADATA_FILE);
    db.files[hash] = {
      name: req.body.name,
      size: file.size,
      mime: file.mimetype,
      uploadedAt: new Date().toISOString(),
      hash: hash,
    };
    fs.writeJsonSync(METADATA_FILE, db);

    res.json({ status: "success", hash });
  });

  // --- Socket.io for Signaling & Status ---
  io.on("connection", (socket) => {
    console.log("Client connected:", socket.id);

    socket.on("join-room", (room) => {
      socket.join(room);
      console.log(`Socket ${socket.id} joined room ${room}`);
      
      const clients = io.sockets.adapter.rooms.get(room);
      if (clients) {
        io.to(room).emit("peer-list", Array.from(clients));
      }
    });

    // Signaling for WebRTC
    socket.on("signal", (payload) => {
      io.to(payload.to).emit("signal", {
        from: socket.id,
        signal: payload.signal,
      });
    });

    socket.on("disconnecting", () => {
      for (const room of socket.rooms) {
        if (room !== socket.id) {
          const clients = io.sockets.adapter.rooms.get(room);
          if (clients) {
            const remaining = Array.from(clients).filter(id => id !== socket.id);
            io.to(room).emit("peer-list", remaining);
          }
        }
      }
    });

    socket.on("disconnect", () => {
      console.log("Client disconnected:", socket.id);
    });
  });

  // --- Vite Middleware ---
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
