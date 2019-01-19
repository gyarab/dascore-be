-- TODO: Is the session info storage mechanism really secure?
-- What about DISCARD?

CREATE OR REPLACE FUNCTION session_user_is_set() RETURNS boolean
LANGUAGE SQL AS $$
    SELECT EXISTS (
        SELECT FROM pg_catalog.pg_class
            WHERE relnamespace = pg_my_temp_schema()
            AND relname = 'temp_session_user');
$$;

CREATE OR REPLACE FUNCTION session_user_set(id_user integer) RETURNS void
LANGUAGE plpgsql AS $$
    BEGIN
        IF session_user_is_set() THEN
            RAISE 'Tried to set user, but user was already set';
        END IF;

        CREATE TEMPORARY TABLE temp_session_user (
            id_user integer
        );

        INSERT INTO temp_session_user (id_user) VALUES (id_user);
    END;
$$;

CREATE OR REPLACE FUNCTION session_user_get() RETURNS integer
LANGUAGE plpgsql AS $$
    BEGIN
        IF NOT session_user_is_set() THEN
            RAISE 'Tried to get user, but user was not set';
        END IF;

        RETURN (SELECT id_user FROM temp_session_user);
    END;
$$;

CREATE OR REPLACE FUNCTION
user_has_permission(_permission_name text, _id_user integer) RETURNS boolean
LANGUAGE plpgsql AS $$
    DECLARE
        permission_id integer;
    BEGIN
        permission_id := (
            SELECT id FROM permissions WHERE name = _permission_name);
        ASSERT permission_id IS NOT NULL, 'Provided permission doesn''t exist';
        RETURN EXISTS (
            -- Timetraveling should not affect permissions in any way
            -- It's not meant to restore the database to a previous state,
            -- but to show a read-only version of that state.
            SELECT FROM user_permissions_current
            WHERE id_permission = permission_id
                AND id_user = _id_user);
    END;
$$;

CREATE OR REPLACE FUNCTION
session_user_has_permission(permission_name text) RETURNS boolean
LANGUAGE sql AS $$
    SELECT user_has_permission(permission_name, session_user_get());
$$;
