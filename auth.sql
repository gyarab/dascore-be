CREATE OR REPLACE FUNCTION session_user_is_set() RETURNS boolean
LANGUAGE plperl STABLE SECURITY DEFINER AS $$
    return (defined $SESSION_USER_ID ? 1 : 0);
$$;

CREATE OR REPLACE FUNCTION session_user_set(integer, text) RETURNS void
LANGUAGE plperl SECURITY DEFINER AS $$
    my ($id_user, $logout_key) = @_;
    if(defined $SESSION_USER_ID) {
        elog(ERROR, "Tried to set user, but user was already set");
        return;
    }
    $SESSION_USER_ID = $id_user;
    $SESSION_LOGOUT_KEY = $logout_key;
$$;

CREATE OR REPLACE FUNCTION session_user_get() RETURNS integer
LANGUAGE plperl STABLE SECURITY DEFINER AS $$
    if(not defined $SESSION_USER_ID) {
        elog(ERROR, "Tried to get user, but user was not set");
        return;
    }
    return $SESSION_USER_ID;
$$;

CREATE OR REPLACE FUNCTION session_logout(text) RETURNS void
LANGUAGE plperl SECURITY DEFINER AS $$
    my ($logout_key) = @_;

    if(not defined $SESSION_LOGOUT_KEY) {
        elog(ERROR, "Tried to logout, but logout key was not set");
        return;
    }
    if($SESSION_LOGOUT_KEY ne $logout_key) {
        undef $SESSION_LOGOUT_KEY;
        elog(ERROR, "Tried to logout, but incorrect logout key "
                      . "was provided. Clearing logout key");
        return;
    }
    undef $SESSION_USER_ID;
    undef $SESSION_LOGOUT_KEY;
$$;

CREATE OR REPLACE FUNCTION
user_has_permission(_permission_name text, _id_user integer) RETURNS boolean
LANGUAGE plpgsql AS $$
    DECLARE
        permission_count integer;
    BEGIN
        permission_count := (
            SELECT COUNT(*) FROM permissions WHERE name = _permission_name);
        ASSERT permission_count = 1, 'Provided permission doesn''t exist';
        RETURN EXISTS (
            -- Timetraveling should not affect permissions in any way
            -- It's not meant to restore the database to a previous state,
            -- but to show a read-only version of that state.
            SELECT FROM user_permissions_current
            WHERE permission = _permission_name AND id_user = _id_user);
    END;
$$;

CREATE OR REPLACE FUNCTION
session_user_has_permission(permission_name text) RETURNS boolean
LANGUAGE sql AS $$
    SELECT user_has_permission(permission_name, session_user_get());
$$;
