-- migrate:up
-- Users table is no longer needed. The weekly digest is posted to a private
-- Telegram channel; Telegram handles subscriptions natively.
DROP TABLE IF EXISTS users;

-- migrate:down
CREATE TABLE users (
    chat_id  BIGINT PRIMARY KEY,
    username TEXT,
    active   BOOLEAN NOT NULL DEFAULT true
);
