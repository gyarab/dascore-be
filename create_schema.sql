CREATE TABLE IF NOT EXISTS users_current (
    id serial PRIMARY KEY,
    username text NOT NULL
);
SELECT version_table('users');
-- XXX: Add permissions when copying this sample
SELECT dascore_setup_table('users', '0=0');

CREATE TABLE IF NOT EXISTS permissions (
    name text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_permissions_current (
    id_user integer REFERENCES users_current NOT NULL,
    permission text REFERENCES permissions NOT NULL,
    PRIMARY KEY (id_user, permission)
);
SELECT version_table('user_permissions');
SELECT dascore_setup_table('user_permissions', '0=0');
