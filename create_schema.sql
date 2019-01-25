CREATE TABLE IF NOT EXISTS users_current (
    id serial PRIMARY KEY,
    username text NOT NULL
);
SELECT version_table('users');

CREATE TABLE IF NOT EXISTS permissions (
    id serial PRIMARY KEY,
    name text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS user_permissions_current (
    id_user integer REFERENCES users_current NOT NULL,
    id_permission integer REFERENCES permissions NOT NULL,
    PRIMARY KEY (id_user, id_permission)
);
SELECT version_table('user_permissions');
