// ~/charbot/ecosystem.config.js
require('dotenv').config({ path: __dirname + '/.env' });
const path = require('path');
const VENV_BIN = path.join(__dirname, '.venv', 'bin');

module.exports = {
  apps: [
    {
      name: "charbot",
      script: "woman.py",
      cwd: __dirname,
      interpreter: "python3",
      env: {
        PATH: VENV_BIN + ":" + process.env.PATH,  // <- venv primeiro
      },
      args: [
        "--download-dir", process.env.DOWNLOAD_DIR || path.join(__dirname, "download"),
        "--log-dir",      process.env.LOG_DIR || path.join(__dirname, "logs"),
        "--headless"
      ],
      autorestart: true, restart_delay: 5000
    },
    {
      name: "char-uploader",
      script: "telegram.py",
      cwd: __dirname,
      interpreter: "python3",
      env: {
        PATH: VENV_BIN + ":" + process.env.PATH,  // <- venv primeiro
        DOWNLOAD_DIR: process.env.DOWNLOAD_DIR || path.join(__dirname, "download"),
        TELEGRAM_TOKEN: process.env.TELEGRAM_TOKEN,
        TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
        WATCH: process.env.WATCH || "1",
        WATCH_INTERVAL: process.env.WATCH_INTERVAL || "10",
        STABLE_AGE: process.env.STABLE_AGE || "20",
        DELETE_AFTER_SEND: process.env.DELETE_AFTER_SEND || "1",
        ENABLE_MTPROTO: process.env.ENABLE_MTPROTO || "0",
        TG_API_ID: process.env.TG_API_ID || "",
        TG_API_HASH: process.env.TG_API_HASH || "",
        MT_PART_KB: process.env.MT_PART_KB || "1024",
        MAX_FILE_GB: process.env.MAX_FILE_GB || "0",
        EXTENSIONS: process.env.EXTENSIONS || ".mp4,.mkv,.mov,.m4v"
      },
      autorestart: true, restart_delay: 5000
    }
  ]
};
