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
user_has_permission(permission_name text, id_user integer) RETURNS boolean
LANGUAGE plpgsql AS $$
    DECLARE
        permission_id integer;
    BEGIN
        permission_id := (
            SELECT id FROM permissions WHERE name = permission_name);
        ASSERT permission_id IS NOT NULL, 'Provided permission doesn''t exist';
        RETURN EXISTS (
            SELECT FROM user_permissions
                WHERE id_permission = permission_id);
    END;
$$;

CREATE OR REPLACE FUNCTION
session_user_has_permission(permission_name text) RETURNS boolean
LANGUAGE sql AS $$
    SELECT user_has_permission(permission_name, session_user_get());
$$;

CREATE OR REPLACE FUNCTION
table_clear_policies(table_name text) RETURNS void
LANGUAGE plpgsql AS $$
    DECLARE
        policy_name text;
	BEGIN
		FOR policy_name IN (SELECT policyname FROM pg_policies
                -- TODO: Not hardcoded schema name
                WHERE schemaname = 'public' AND tablename = table_name) LOOP
			EXECUTE format('DROP POLICY %I ON %I', policy_name, table_name);
		END LOOP;
	END;
$$;
