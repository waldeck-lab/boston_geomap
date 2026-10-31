"""One-time, auth-tables-only snapshot. Production is opened read-only."""
import json
import os
import secrets
import sqlite3
import unicodedata
from pathlib import Path

root=Path('/proj/boston/beta/wcr')
target=root/'stage/auth/accounts.sqlite'
settings_path=root/'ops/auth-settings.json'
if target.exists() or settings_path.exists():raise SystemExit('Auth store already exists; refusing to replace it')
target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
source=sqlite3.connect('file:/proj/boston/repo/wcr/backend/resources/db/wcr_db.sqlite?mode=ro',uri=True)
db=sqlite3.connect(target)
tables=['users','groups','user_groups','auth_clients','auth_grants','group_grants']
try:
    source.execute('BEGIN')
    with db:
        for table in tables:
            schema=source.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()[0]
            db.execute(schema)
            rows=source.execute('SELECT * FROM '+table).fetchall()
            if rows:db.executemany('INSERT INTO '+table+' VALUES('+','.join('?' for _ in rows[0])+')',rows)
        db.execute('ALTER TABLE users ADD COLUMN username_key TEXT')
        for uid,name in db.execute('SELECT id,username FROM users').fetchall():
            db.execute('UPDATE users SET username_key=? WHERE id=?',(unicodedata.normalize('NFKC',name.strip()).casefold(),uid))
        db.execute('CREATE UNIQUE INDEX users_username_key ON users(username_key)')
        db.execute('''CREATE TABLE auth_rate_limits(scope TEXT NOT NULL,subject TEXT NOT NULL,
            window INTEGER NOT NULL,attempts INTEGER NOT NULL,expires_at INTEGER NOT NULL,
            PRIMARY KEY(scope,subject,window))''')
    db.execute('PRAGMA journal_mode=WAL')
    settings=dict(database=str(target),session_secret=secrets.token_hex(48),internal_secret=secrets.token_hex(48))
    fd=os.open(settings_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:json.dump(settings,f)
    target.chmod(0o600)
    print('Created isolated auth store; copied accounts:',db.execute('SELECT count(*) FROM users').fetchone()[0])
finally:db.close();source.close()
