CREATE TABLE IF NOT EXISTS admin_sig (
    sig_code TEXT PRIMARY KEY,
    sig_name TEXT NOT NULL,
    sido_code TEXT NOT NULL,
    sido_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_sido (
    sido_code TEXT PRIMARY KEY,
    sido_name TEXT NOT NULL
);
