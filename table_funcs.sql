CREATE OR REPLACE FUNCTION timepoint_is_set() RETURNS boolean
LANGUAGE SQL AS $$
    SELECT current_setting('dascore.timepoint', TRUE) IS NOT NULL
        AND current_setting('dascore.timepoint', TRUE) <> '';
$$;


-- Sets a table up to keep history.
-- It expects a table named NAME_current to exist, which will be used to keep
-- the current rows.
-- It creates a table named NAME_history for keeping historical records
CREATE OR REPLACE FUNCTION version_table(table_name text) RETURNS void
LANGUAGE plpgsql AS $func$
BEGIN
    EXECUTE format($$
        ALTER TABLE %1$I_current
        ADD COLUMN sys_period tstzrange
            NOT NULL
            DEFAULT tstzrange(current_timestamp, null);

        CREATE TABLE %1$I_history (LIKE %1$I_current);

        CREATE TRIGGER versioning_trigger
        BEFORE INSERT OR UPDATE OR DELETE ON %1$I_current
        FOR EACH ROW EXECUTE PROCEDURE
            versioning('sys_period', '%1$I_history', true);
    $$, table_name);
END;
$func$;

-- Creates a view for unified access to a pair of tables created by
-- version_table()
-- The permissions are SQL expressions returning a bool that decide if an
-- operation is permitted or not.
-- "select_perm" is applied for all operations, not just SELECT.
-- "modify_perm" is applied to all modifying operations.
CREATE OR REPLACE FUNCTION dascore_setup_table(
    table_name text,
    select_perm text,
    modify_perm text DEFAULT null,
    insert_perm text DEFAULT null,
    update_perm text DEFAULT null,
    delete_perm text DEFAULT null)
RETURNS void
LANGUAGE plpgsql AS $func$
DECLARE
    data_cols_trigstr text;
    trig_cond text;
    trig_setstmt text;
    select_cond text;
    insert_cond text;
    insert_check text;
    update_cond text;
    update_check text;
    delete_cond text;
    delete_check text;
BEGIN
    -- TODO: Schema
    data_cols_trigstr := (
        SELECT string_agg(CASE
            -- This relies on coalesce acting "lazily" and only evaluating the
            -- second option if the first one is NULL
            WHEN column_default IS NOT NULL THEN
                format('COALESCE(NEW.%I, %s)', column_name, column_default)
            ELSE format('NEW.%I', column_name)
        END, ', ')
        FROM information_schema.columns AS c
        WHERE c.table_name = format('%I_current', $1));

    trig_cond := (
        SELECT string_agg(format('OLD.%1$I = %1$I', a.attname), ' AND ')
        FROM pg_index AS i
        JOIN pg_attribute AS a
            ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = format('%I_current', table_name)::regclass
            AND i.indisprimary);

    trig_setstmt := (
        SELECT string_agg(format('%1$I = NEW.%1$I', column_name), ', ')
        FROM information_schema.columns
        WHERE columns.table_name = format('%I_current', $1));


    select_cond := (
        CASE
            WHEN select_perm IS NOT NULL THEN format('WHERE (%s)', select_perm)
            ELSE ''
        END);

    EXECUTE format($$
        CREATE OR REPLACE VIEW %1$I
        WITH (security_barrier = true) AS
        SELECT * FROM (
            SELECT * FROM %1$I_current
                -- TODO: Is there a performance penalty for calling
                -- current_setting so much?
                WHERE NOT timepoint_is_set()
                    OR sys_period @> current_setting('dascore.timepoint', TRUE)::timestamptz
            UNION SELECT * FROM %1$I_history
                WHERE timepoint_is_set()
                    AND sys_period @> current_setting('dascore.timepoint', TRUE)::timestamptz)
            AS ROW
        %2$s;
    $$, table_name, select_cond);


    insert_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (insert_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    insert_check := (
        CASE
            WHEN insert_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on INSERT into %%', %L;
                END IF;
            $$, insert_cond, table_name)
        END);

    EXECUTE format($$
        CREATE FUNCTION %1$I_insert_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR NEW;
        BEGIN
            -- TODO: Better name?
            IF timepoint_is_set() THEN
                RAISE EXCEPTION
                    'Can''t insert into temporal table %% when timetraveling',
                    %1$L;
            END IF;
            %3$s
            INSERT INTO %1$I_current VALUES (%2$s);
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_insert
        INSTEAD OF INSERT ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_insert_trigger();
    $$, table_name, data_cols_trigstr, insert_check);


    update_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (update_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    update_check := (
        CASE
            WHEN update_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on UPDATE of %%', %L;
                END IF;
            $$, update_cond, table_name)
        END);

    -- TODO: Permission checks should run against both OLD and NEW
    EXECUTE format($$
        CREATE FUNCTION %1$I_update_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR OLD;
        BEGIN
            IF timepoint_is_set() THEN
                RAISE EXCEPTION
                    'Can''t update temporal table %% when timetraveling', %1$L;
            END IF;
            %4$s
            UPDATE %1$I_data SET %2$s WHERE %3$s;
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_update
        INSTEAD OF UPDATE ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_update_trigger();
    $$, table_name, trig_setstmt, trig_cond, update_check);


    delete_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (delete_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    delete_check := (
        CASE
            WHEN delete_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on DELETE in %%', %L;
                END IF;
            $$, delete_cond, table_name)
        END);

    EXECUTE format($$
        CREATE FUNCTION %1$I_delete_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR OLD;
        BEGIN
            IF timepoint_is_set() THEN
                RAISE EXCEPTION
                    'Can''t delete from temporal table %% when timetraveling',
                    %1$L;
            END IF;
            %2$s
            DELETE FROM %1$I_current WHERE %3$s;
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_delete
        INSTEAD OF DELETE ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_delete_trigger()
    $$, table_name, delete_check, trig_cond);
END;
$func$;

-- Creates a view that enforces permissions and otherwise mirrors a table
-- called NAME_data.
CREATE OR REPLACE FUNCTION dascore_setup_table_unversioned(
    table_name text,
    select_perm text,
    modify_perm text DEFAULT null,
    insert_perm text DEFAULT null,
    update_perm text DEFAULT null,
    delete_perm text DEFAULT null)
RETURNS void
LANGUAGE plpgsql AS $func$
DECLARE
    data_cols_trigstr text;
    trig_cond text;
    trig_setstmt text;
    select_cond text;
    insert_cond text;
    insert_check text;
    update_cond text;
    update_check text;
    delete_cond text;
    delete_check text;
BEGIN
    -- TODO: Schema
    data_cols_trigstr := (
        SELECT string_agg(CASE
            -- This relies on coalesce acting "lazily" and only evaluating the
            -- second option if the first one is NULL
            WHEN column_default IS NOT NULL THEN
                format('COALESCE(NEW.%I, %s)', column_name, column_default)
            ELSE format('NEW.%I', column_name)
        END, ', ')
        FROM information_schema.columns AS c
        WHERE c.table_name = format('%I_data', $1));

    trig_cond := (
        SELECT string_agg(format('OLD.%1$I = %1$I', a.attname), ' AND ')
        FROM pg_index AS i
        JOIN pg_attribute AS a
            ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = format('%I_data', table_name)::regclass
            AND i.indisprimary);

    trig_setstmt := (
        SELECT string_agg(format('%1$I = NEW.%1$I', column_name), ', ')
        FROM information_schema.columns
        WHERE columns.table_name = format('%I_data', $1));


    select_cond := (
        CASE
            WHEN select_perm IS NOT NULL THEN format('WHERE (%s)', select_perm)
            ELSE ''
        END);

    EXECUTE format($$
        CREATE OR REPLACE VIEW %1$I
        WITH (security_barrier = true) AS
            SELECT * FROM %1$I_data AS ROW
            %2$s;
    $$, table_name, select_cond);


    insert_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (insert_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    insert_check := (
        CASE
            WHEN insert_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on INSERT into %%', %L;
                END IF;
            $$, insert_cond, table_name)
        END);

    EXECUTE format($$
        CREATE FUNCTION %1$I_insert_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR NEW;
        BEGIN
            %3$s
            INSERT INTO %1$I_data VALUES (%2$s);
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_insert
        INSTEAD OF INSERT ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_insert_trigger();
    $$, table_name, data_cols_trigstr, insert_check);


    update_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (update_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    update_check := (
        CASE
            WHEN update_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on UPDATE of %%', %L;
                END IF;
            $$, update_cond, table_name)
        END);

    EXECUTE format($$
        CREATE FUNCTION %1$I_update_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR OLD;
        BEGIN
            %4$s
            UPDATE %1$I_data SET %2$s WHERE %3$s;
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_update
        INSTEAD OF UPDATE ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_update_trigger();
    $$, table_name, trig_setstmt, trig_cond, update_check);


    delete_cond := (
        SELECT string_agg(format('(%s)', expr), ' AND ')
        FROM (VALUES (select_perm), (modify_perm), (delete_perm))
            AS perms (expr)
        WHERE expr IS NOT NULL);

    delete_check := (
        CASE
            WHEN delete_cond IS NULL THEN ''
            ELSE format($$
                IF NOT (%s) THEN
                    RAISE EXCEPTION 'Permission denied on DELETE in %%', %L;
                END IF;
            $$, delete_cond, table_name)
        END);

    EXECUTE format($$
        CREATE FUNCTION %1$I_delete_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $func2$
        DECLARE
            ROW ALIAS FOR OLD;
        BEGIN
            %2$s
            DELETE FROM %1$I_data WHERE %3$s;
            RETURN NEW;
        END
        $func2$;

        CREATE TRIGGER temporal_delete
        INSTEAD OF DELETE ON %1$I
        FOR EACH ROW
        EXECUTE PROCEDURE %1$I_delete_trigger()
    $$, table_name, delete_check, trig_cond);
END;
$func$;
