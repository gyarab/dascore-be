CREATE OR REPLACE FUNCTION timepoint_is_set() RETURNS boolean
LANGUAGE SQL AS $$
	SELECT current_setting('dascore.timepoint', TRUE) IS NOT NULL
		AND current_setting('dascore.timepoint', TRUE) <> '';
$$;

CREATE OR REPLACE FUNCTION version_table(table_name text) RETURNS void
LANGUAGE plpgsql AS $func$
DECLARE
	data_cols_trigstr text;
	trig_cond text;
	trig_setstmt text;
BEGIN
	-- TODO: Schema
	SELECT string_agg(CASE
		-- This relies on coalesce acting "lazily" and only evaluating the
		-- second option if the first one is NULL
		WHEN column_default IS NOT NULL THEN
			format('COALESCE(NEW.%I, %s)', column_name, column_default)
		ELSE format('NEW.%I', column_name)
	END, ', ')
	INTO data_cols_trigstr
	FROM information_schema.columns
	WHERE columns.table_name = format('%I_current', $1);

	SELECT string_agg(format('OLD.%1$I = %1$I', a.attname), ' AND ')
	INTO trig_cond
	FROM pg_index AS i
	JOIN pg_attribute AS a
		ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
	WHERE i.indrelid = format('%I_current', table_name)::regclass
		AND i.indisprimary;

	SELECT string_agg(format('%1$I = NEW.%1$I', column_name), ', ')
	INTO trig_setstmt
	FROM information_schema.columns
	WHERE columns.table_name = format('%I_current', $1);

	EXECUTE format($$
		ALTER TABLE %I_current
		ADD COLUMN sys_period tstzrange
			NOT NULL
			DEFAULT tstzrange(current_timestamp, null);

		CREATE TABLE %1$I_history (LIKE %1$I_current);

		CREATE TRIGGER versioning_trigger
		BEFORE INSERT OR UPDATE OR DELETE ON %1$I_current
		FOR EACH ROW EXECUTE PROCEDURE
			versioning('sys_period', '%1$I_history', true);

		CREATE VIEW %1$I AS
		SELECT * FROM %1$I_current
			-- TODO: Is there a performance penalty for calling
			-- current_setting so much?
			WHERE NOT timepoint_is_set()
				OR sys_period @> current_setting('dascore.timepoint', TRUE)::timestamptz
		UNION SELECT * FROM %1$I_history
			WHERE timepoint_is_set()
				AND sys_period @> current_setting('dascore.timepoint', TRUE)::timestamptz;
		-- Unfortunately, row-level security becomes somewhat of a mess when
		-- views get involved. For example, views consider RLS policies as if
		-- they are being run by the user who created the view. As that's most
		-- likely a superuser, views ignore RLS policies. This can be fixed
		-- by reassigning the view to another user who can't ignore RLS.
		-- This is pretty error prone. Security in SQL is supposed to prevent
		-- vulnerabilities, not create a new class of them, so we'll have to
		-- come up with a robust solution (or get Postgres "fixed").
		GRANT ALL ON TABLE %1$I_current TO viewowner;
		GRANT ALL ON TABLE %1$I_history TO viewowner;
		ALTER VIEW %1$I OWNER TO viewowner;
		GRANT ALL ON TABLE %1$I TO dvdkon;

		CREATE FUNCTION %1$I_insert_trigger() RETURNS trigger
		LANGUAGE plpgsql AS $func2$
		BEGIN
			-- TODO: Better name?
			IF timepoint_is_set() THEN
				RAISE EXCEPTION
					'Can''t insert into temporal table %% when timetraveling',
					%1$L;
			END IF;
			INSERT INTO %1$I_current VALUES (%2$s);
			RETURN NEW;
		END
		$func2$;

		CREATE TRIGGER temporal_insert
		INSTEAD OF INSERT ON %1$I
		FOR EACH ROW
		EXECUTE PROCEDURE %1$I_insert_trigger();

		CREATE FUNCTION %1$I_update_trigger() RETURNS trigger
		LANGUAGE plpgsql AS $func2$
		BEGIN
			IF timepoint_is_set() THEN
				RAISE EXCEPTION
					'Can''t update temporal table %% when timetraveling', %1$L;
			END IF;
			UPDATE %1$I_current SET %4$s WHERE %3$s;
			RETURN NEW;
		END
		$func2$;

		CREATE TRIGGER temporal_update
		INSTEAD OF UPDATE ON %1$I
		FOR EACH ROW
		EXECUTE PROCEDURE %1$I_update_trigger();

		CREATE FUNCTION %1$I_delete_trigger() RETURNS trigger
		LANGUAGE plpgsql AS $func2$
		BEGIN
			IF timepoint_is_set() THEN
				RAISE EXCEPTION
					'Can''t delete from temporal table %% when timetraveling',
					%1$L;
			END IF;
			DELETE FROM %1$I_current WHERE %3$s;
			RETURN NEW;
		END
		$func2$;

		CREATE TRIGGER temporal_delete
		INSTEAD OF DELETE ON %1$I
		FOR EACH ROW
		EXECUTE PROCEDURE %1$I_delete_trigger()
	$$, table_name, data_cols_trigstr, trig_cond, trig_setstmt);
END;
$func$;
