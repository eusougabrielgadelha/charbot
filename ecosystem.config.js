{
  name: "char-uploader",
  script: "telegram.py",
  cwd: process.env.HOME + "/charbot",
  interpreter: process.env.HOME + "/charbot/.venv/bin/python",
  env: {
    DOWNLOAD_DIR: process.env.HOME + "/charbot/download",
    TELEGRAM_TOKEN: "1929155873:AAHF4DApvKGOFNOaKHJ5PwKFuaPkKEkE0uE",
    TELEGRAM_CHAT_ID: "-1001812955444",

    // comportamento:
    WATCH: "1",
    WATCH_INTERVAL: "10",
    STABLE_AGE: "20",
    DELETE_AFTER_SEND: "1",

    // (opcional) MTProto para arquivos gigantes:
    ENABLE_MTPROTO: "0",
    TG_API_ID: "SEU_API_ID",
    TG_API_HASH: "SEU_API_HASH",
    MT_PART_KB: "1024"
  },
  autorestart: true,
  restart_delay: 5000
}
