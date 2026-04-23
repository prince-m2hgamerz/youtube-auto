-- Bot settings table for persistent configuration across restarts
-- Run this in your Supabase SQL Editor

CREATE TABLE IF NOT EXISTS bot_settings (
    id SERIAL PRIMARY KEY,
    setting_name TEXT NOT NULL UNIQUE,
    setting_value TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Disable RLS so the anon key can read/write (safe for single-admin bot)
ALTER TABLE bot_settings DISABLE ROW LEVEL SECURITY;

-- Insert default settings
INSERT INTO bot_settings (setting_name, setting_value)
VALUES 
    ('source_channels', '[]'),
    ('auto_upload_visibility', 'public'),
    ('auto_upload_times', '07:15,19:15'),
    ('uploaded_shorts_ids', '[]'),
    ('scheduler_last_runs', '{}')
ON CONFLICT (setting_name) DO NOTHING;
