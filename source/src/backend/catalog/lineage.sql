/*
 * 
 * functions and aggregations to compute lineage and p
 *
 * src/backEND/catalog/lineage.sql
 */

/*
 * FUNCTION and AGGREGATE to concat lineage with OR:
 * 1,2,3,4 => 1+2+3+4
 */
/* make a null if b=''*/
-- CREATE FUNCTION nullify(a int, b text) RETURNS int AS $$
-- SELECT
--   CASE
--   WHEN $2 = '' THEN NULL
--   ELSE $1
-- END;
-- $$ LANGUAGE sql;
-- CREATE AGGREGATE lineage_or(text) ( 
--   sfunc       = concat_lineage_or, 
--   stype       = text, 
--   initcond    = '' 
-- );
-- COMMENT ON FUNCTION nullify(a text, b text) IS 'nullify attributes for outer joins';
-- COMMENT ON AGGREGATE lineage_or(text) IS 'lineage aggregate for OR';

----------------------------------------------------------------------------------------

CREATE FUNCTION lineage_for_windows(a text, b text, c anyelement) RETURNS text AS $$
SELECT
  CASE
  WHEN ($1 IS NULL OR $1 = '') AND ($2 IS NOT NULL AND $2 <> '')  THEN '(' || $2 || ')'   -- unmatched windows
  WHEN ($2 IS NULL OR $2 = '') AND ($1 IS NOT NULL AND $1 <> '')  THEN '(' || $1 || ')'
  WHEN ($3 IS NULL) THEN '(' || $1 || ') * (-(' || $2 || '))'   -- negating windows
  ELSE  '(' || $1 || ') * (' || $2 || ')'   -- overlapping windows
END;
$$ LANGUAGE sql;
-- CREATE AGGREGATE lineage_windows(text) ( 
--   sfunc       = lineage_for_windows, 
--   stype       = text, 
--   initcond    = '' 
-- );
COMMENT ON FUNCTION lineage_for_windows(a text, b text, c anyelement) IS 'lineage for windows';
-- COMMENT ON AGGREGATE lineage_windows(text) IS 'lineage aggregate for windows';

---------------------------------------------------------------------------------------

CREATE FUNCTION concat_lineage_or(a text, b text) RETURNS text AS $$
SELECT
  CASE
  WHEN ($1 IS NULL OR $1 = '') AND ($2 IS NOT NULL AND $2 <> '')  THEN '(' || $2 || ')'
  WHEN ($2 IS NULL OR $2 = '') AND ($1 IS NOT NULL AND $1 <> '')  THEN '(' || $1 || ')'
  ELSE  '(' || $1 || ') + (' || $2 || ')' 
END;
$$ LANGUAGE sql;
CREATE AGGREGATE lineage_or(text) ( 
  sfunc       = concat_lineage_or, 
  stype       = text, 
  initcond    = '' 
);
COMMENT ON FUNCTION concat_lineage_or(a text, b text) IS 'lineage concat for OR';
COMMENT ON AGGREGATE lineage_or(text) IS 'lineage aggregate for OR';

/*
 * FOR GROUP BY - AGGREGATE
 * FUNCTION and AGGREGATE to concat lineage with AND:
 * 1,2,3,4 => (1)*(2)*(3)*(4)
 */
CREATE FUNCTION concat_lineage_and(a text, b text) RETURNS text AS $$
SELECT 
  CASE 
  WHEN ($1 IS NULL OR $1 = '') AND ($2 IS NOT NULL AND $2 <> '')  THEN '(' || $2 || ')'
  WHEN ($2 IS NULL OR $2 = '') AND ($1 IS NOT NULL AND $1 <> '')  THEN '(' || $1 || ')'
  ELSE  '(' || $1 || ') * (' || $2 || ')' 
END; 
$$ LANGUAGE sql;
CREATE AGGREGATE lineage_and(text) ( 
  sfunc       = concat_lineage_and, 
  stype       = text, 
  initcond    = '' 
);
COMMENT ON FUNCTION concat_lineage_and(a text, b text) IS 'lineage concat for AND';
COMMENT ON AGGREGATE lineage_and(text) IS 'lineage aggregate for AND';


/*
 * FOR GROUP BY - AGGREGATE
 * FUNCTION and AGGREGATE to concat lineage with AND:
 * 1,2,3,4 => (1)*(2)*(3)*(4)
 */
CREATE FUNCTION concat_lineage_andnot(a text, b text) RETURNS text AS $$
SELECT 
  CASE 
  WHEN ($2 IS NULL OR $2 = '') AND ($1 IS NOT NULL AND $1 <> '')  THEN '(' || $1 || ')'
  ELSE  '(' || $1 || ') * (-(' || $2 || '))' 
END; 
$$ LANGUAGE sql;
CREATE AGGREGATE lineage_andnot(text) ( 
  sfunc       = concat_lineage_andNot, 
  stype       = text, 
  initcond    = '' 
);
COMMENT ON FUNCTION concat_lineage_andNot(a text, b text) IS 'lineage concat for AND NOT';
COMMENT ON AGGREGATE lineage_andNot(text) IS 'lineage aggregate for AND NOT';


/*
 * FUNCTION to concat two lineages with AND:
 */
-- CREATE FUNCTION concat_lineage_and2(a text, b text) RETURNS text AS $$
-- SELECT 
--   CASE 
--   WHEN $1 IS NULL OR $1 = '' THEN '(' || $2 || ')' 
--   WHEN $2 IS NULL OR $2 = '' THEN '(' || $1 || ')' 
--   ELSE '(' || $1 || ')*(' || $2 || ')' 
-- END; 
-- $$ LANGUAGE sql;
-- COMMENT ON FUNCTION concat_lineage_and2(a text, b text) IS 'lineage concat for AND';

-- /*
--  * FUNCTION and AGGREGATE to concat lineage with AND NOT:
--  * 1,2,3,4 => (1)*(-2)*(-3)*(-4)
--  */
-- CREATE FUNCTION concat_lineage_notand(a text, b text) RETURNS text AS $$
-- SELECT 
--   CASE 
--   WHEN $1 IS NULL OR $1 = '' THEN '(-(' || $2 || '))' 
--   WHEN $2 IS NULL OR $2 = '' THEN '(-(' || $1 || '))' 
--   ELSE $1 || '*(-(' || $2 || '))' 
-- END; 
-- $$ LANGUAGE sql;
-- CREATE AGGREGATE lineage_notand(text) ( 
--   sfunc       = concat_lineage_notand, 
--   stype       = text, 
--   initcond    = '' 
-- );
-- COMMENT ON FUNCTION concat_lineage_notand(a text, b text) IS 'lineage concat for NOT AND';
-- COMMENT ON AGGREGATE lineage_notand(text) IS 'lineage aggregate for NOT AND';

-- /*
--  * FUNCTION and AGGREGATE to RETURN the first elment of the group:
--  * 1,2,3,4 => 1
--  */
-- CREATE FUNCTION lineage_first_agg ( anyelement, anyelement ) 
-- RETURNS anyelement LANGUAGE sql IMMUTABLE STRICT AS $$ 
--   SELECT $1; 
-- $$;
-- CREATE AGGREGATE lineage_first ( 
--   sfunc    = lineage_first_agg, 
--   basetype = anyelement, 
--   stype    = anyelement 
-- );
-- COMMENT ON FUNCTION lineage_first_agg(anyelement, anyelement) IS 'lineage first function';
-- COMMENT ON AGGREGATE lineage_first(anyelement) IS 'lineage first aggregate';

/*
 * FUNCTION that returns the p-Value of a base tuple lineage
 */
CREATE FUNCTION lineage_prob(lineage_expr text) RETURNS numeric AS $$
  DECLARE
    res  numeric;
    tt text;
    tuple text;
    b boolean;
  BEGIN
	EXECUTE 'SELECT ' || split_part(lineage_expr, '.', 1) || '::regclass ' INTO tt;
	EXECUTE 'select case when exists (select * from pg_attribute where attrelid = ''' || split_part(lineage_expr, '.', 1) || ''' and attname = ''p'') then 1 ELSE 0 END AS t' INTO b;
	IF NOT b THEN
	  RAISE EXCEPTION 'Table ''%'' has no p-Value', tt 
      USING HINT = 'Please check your user table';
	END IF;
    tuple := split_part(lineage_expr, '.', 2);
    EXECUTE 'SELECT p FROM ' || tt || ' WHERE oid = ''' || tuple || '''' INTO res;
  RETURN res;
END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_prob(text) IS 'returns p-Value of tuple with the given lineage';

/*
 * FUNCTION that returns the p-Value to the power of the duration of a base tuple lineage
 */
CREATE FUNCTION lineage_probt(lineage_expr text) RETURNS double precision AS $$
  DECLARE
    res  double precision;
    d double precision;
    tt text;
    tuple text;
    b boolean;
    ts date;
    te date;
  BEGIN
	EXECUTE 'SELECT ' || split_part(lineage_expr, '.', 1) || '::regclass ' INTO tt;
	EXECUTE 'select case when exists (select * from pg_attribute where attrelid = ''' || split_part(lineage_expr, '.', 1) || ''' and attname = ''p'') then 1 ELSE 0 END as t' INTO b;
	IF NOT b THEN
	  RAISE EXCEPTION 'Table ''%'' has no p-Value', tt 
      USING HINT = 'Please check your user table';
	END IF;
    tuple := split_part(lineage_expr, '.', 2);
    EXECUTE 'SELECT p, ts, te FROM ' || tt || ' WHERE oid = ''' || tuple || '''' INTO res, ts, te;
    d := te-ts;
  RETURN power(res, 1/d);
END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_probt(text) IS 'returns p-Value to the power of duration of tuple with the given lineage';

/*
 * FUNCTION that returns an array of all base tuple lineages that are defined in the given lineage
 */
CREATE FUNCTION lineage_vars(lineage_expr text) RETURNS text[] AS $$
  DECLARE
    array1 text[];
    array2 text[];
    element text;
  BEGIN
	array1 := string_to_array(regexp_replace(lineage_expr, '[*()+-]', '~^~', 'g'), '~^~');
	array2 := '{}';
    FOREACH element in array array1 LOOP
      IF char_length(element) != 0 and not (array2 @> array_appEND('{}', element)) then
        array2 := array2 || element;
      END IF;
    END LOOP;
    RETURN array2;
  END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_vars(text) IS 'returns an array of all base tuple lineages that are defined in the given lineage';

/*
 * FUNCTION that returns an array of all operators, brackets and base tuple lineages that are defined in the given lineage
 */
CREATE FUNCTION lineage_tokenize(lineage_expr text) RETURNS text[] AS $$
  DECLARE
    array1 text[];
    array2 text[];
    element text;
  BEGIN
	array1 := string_to_array(regexp_replace(lineage_expr, '([^0-9.-])', '~^~\1~^~', 'g'), '~^~');
	array2 := '{}';
    FOREACH element in array array1 LOOP
      IF char_length(element) != 0 then
        array2 := array2 || element;
      END IF;
    END LOOP;
    RETURN array2;
  END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_tokenize(text) IS 'returns an array of all operators, brackets and base tuple lineages that are defined in the given lineage';

/*
 * FUNCTION that returns an array of the given lineage in postfix transformation
 */
CREATE FUNCTION lineage_postfix(lineage_expr text) RETURNS text[] AS $$
  DECLARE
    token text[];
    stack text[];
    new_stack text[];
    output text[];
    element text;
    stack_element text;
    found boolean;
  BEGIN
    token := lineage_tokenize(lineage_expr);
    FOREACH element in array token LOOP
	--RAISE NOTICE '--------------------------------------------------';
	--RAISE NOTICE 'start with element %', element;
	IF element is not distinct from '(' then
		stack := element || stack;
	ELSIF element is not distinct from '+' then
		new_stack := '{}';
		found := false;
		IF array_length(stack, 1) > 0 then
			FOREACH stack_element in array stack LOOP
				IF (stack_element is not distinct from '+' or stack_element is not distinct from '*' or stack_element is not distinct from '-') and not found then
					output := output || stack_element;
				ELSIF not found then
					found := true;
					new_stack := new_stack || stack_element;
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
			END LOOP;
		END IF;
		stack := new_stack;
		stack := element || stack;
	ELSIF element is not distinct from '*' then
		new_stack := '{}';
		found := false;
		IF array_length(stack, 1) > 0 then
			FOREACH stack_element in array stack LOOP
				IF (stack_element is not distinct from '*' or stack_element is not distinct from '-') and not found then
					output := output || stack_element;
				ELSIF not found then
					found := true;
					new_stack := new_stack || stack_element;
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
			END LOOP;
		END IF;
		stack := new_stack;
		stack := element || stack;
	ELSIF element is not distinct from '-' then	
		new_stack := '{}';
		found := false;
		IF array_length(stack, 1) > 0 then
			FOREACH stack_element in array stack LOOP
				IF (stack_element is not distinct from '-') and not found then
					output := output || stack_element;
				ELSIF not found then
					found := true;
					new_stack := new_stack || stack_element;
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
			END LOOP;
		END IF;
		stack := new_stack;
		stack := element || stack;
	ELSIF element is not distinct from ')' then
		new_stack := '{}';
		found := false;
		IF array_length(stack, 1) > 0 then
			FOREACH stack_element in array stack LOOP
				IF stack_element is distinct from '(' and not found then
					output := output || stack_element;
				ELSIF not found then
					found := true;
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
			END LOOP;
		END IF;
		stack := new_stack;
	ELSE
		output := output || element;
	END IF;
	--IF array_length(stack, 1) > 0 then
	--	FOREACH stack_element in array stack LOOP
	--		RAISE NOTICE 'stack element %', stack_element;
	--	END LOOP;
	--END IF;
	--IF array_length(output, 1) > 0 then
	--	FOREACH stack_element in array output LOOP
	--		RAISE NOTICE 'output element %', stack_element;
	--	END LOOP;
	--END IF;
	
    END LOOP;
    IF array_length(stack, 1) > 0 then
	FOREACH stack_element in array stack LOOP
		IF element is distinct from '(' then
			output := output || stack_element;
		END IF;
	END LOOP;
    END IF;
    --RAISE NOTICE 'output % ', output;
    --output = '{74430.74436,74450.74456,*,74430.74439,74450.74458,*,-,+}';
    RETURN output;
  END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_postfix(text) IS 'returns an array of the given lineage in postfix transformation';

/*
 * FUNCTION that returns true if the given var is set to true in vars
 * vars in form: var1=0,var2=1,...
 */
CREATE FUNCTION lineage_true_or_false(vars text[], var text) RETURNS boolean AS $$
  DECLARE
    element text;
  BEGIN
	  FOREACH element in array vars LOOP
	    IF position(var in element) = 1 then
	      IF substring(element from position('=' in element)+1) is not distinct from '0' then
	        RETURN false;
	      ELSE
	        RETURN true;
	      END IF;
	    END IF;
	  END LOOP;
	  RAISE NOTICE 'ERROR: The given variable does not exist';
  END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_true_or_false(text[], text) IS 'returns true if the given var is set to true in vars';

/*
 * FUNCTION that evaluates if the given lineage is true for the given variables
 */
CREATE FUNCTION lineage_evaluate(lineage_expr text, vars text[]) RETURNS boolean AS $$
  DECLARE
    postfix text[];
    element text;
    stack boolean[];
    new_stack boolean[];
    stack_element boolean;
    i integer;
    prev boolean;
  BEGIN
	postfix := '{}';
	postfix := postfix || lineage_postfix(lineage_expr);
	stack := '{}';
	FOREACH element in array postfix LOOP
		IF element is not distinct from '*' then
			new_stack := '{}';
			i := 0;
			FOREACH stack_element in array stack LOOP
				IF i = 0 then
					prev := stack_element;
				ELSIF i = 1 then
					new_stack := new_stack || (prev and stack_element);
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
				i := i + 1;
			END LOOP;
			stack := new_stack;
		ELSIF element is not distinct from '+' then
			new_stack := '{}';
			i := 0;
			FOREACH stack_element in array stack LOOP
				IF i = 0 then
					prev := stack_element;
				ELSIF i = 1 then
					new_stack := new_stack || (prev or stack_element);
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
				i := i + 1;
			END LOOP;
			stack := new_stack;
		ELSIF element is not distinct from '-' then
			new_stack := '{}';
			i := 0;
			FOREACH stack_element in array stack LOOP
				IF i = 0 then
					new_stack := new_stack || (not (stack_element));
				ELSE
					new_stack := new_stack || stack_element;
				END IF;
				i := i + 1;
			END LOOP;
			stack := new_stack;
		ELSE
			stack := lineage_true_or_false(vars, element) || stack;
	
		END IF;
	END LOOP;
	RETURN stack[1];
  END;
$$ language plpgsql;
COMMENT ON FUNCTION lineage_evaluate(text, text[]) IS 'evaluates if the given lineage is true for the given variables';

/*
 * FUNCTION that computes p for the given lineage
 */
CREATE FUNCTION lineage_conf(lineage_expr text) RETURNS numeric AS $$
  DECLARE
    vars text[];
    var text;
    bs text[];
    s numeric;
    ss numeric;
    n integer;
    i integer;
    j integer;
  BEGIN
	vars := lineage_vars(lineage_expr);
	n := array_length(vars, 1);
	i := 0;
	s := 0;
	WHILE i < power(2, n) LOOP
	  bs := '{}';
	  j := 0;
	  FOREACH var in array vars LOOP
	    IF (floor(i / power(2, n - 1 - j) )::integer % 2 = 0 ) then
	      bs := bs || (var ||'=1');
	    ELSE
	      bs := bs || (var ||'=0');
	    END IF;
	    j := j + 1;
	  END LOOP;
	  
	  IF lineage_evaluate(lineage_expr, bs) then
	    ss := 1;
	    FOREACH var in array vars LOOP
	      IF lineage_true_or_false(bs, var) then
	        ss := ss * lineage_prob(var);
	      ELSE
	        ss := ss * (1- lineage_prob(var));
	      END IF;
	    END LOOP;
	    s := s + ss;
	  END IF;
	  i := i + 1;
	END LOOP;
	RETURN s;
  END 
$$ language plpgsql;
COMMENT ON FUNCTION lineage_conf(text) IS 'computes p for the given lineage';

/*
 * FUNCTION that computes p (normalized by the duration) for the given lineage
 */
CREATE FUNCTION lineage_conft(lineage_expr text, ts date, te date) RETURNS numeric AS $$
  DECLARE
    vars text[];
    var text;
    bs text[];
    s double precision;
    ss double precision;
    n integer;
    i integer;
    j integer;
    d integer;
  BEGIN
	vars := lineage_vars(lineage_expr);
	n := array_length(vars, 1);
	i := 0;
	s := 0;
	WHILE i < power(2, n) LOOP
	  bs := '{}';
	  j := 0;
	  FOREACH var in array vars LOOP
	    IF (floor(i / power(2, n - 1 - j) )::integer % 2 = 0 ) then
	      bs := bs || (var ||'=1');
	    ELSE
	      bs := bs || (var ||'=0');
	    END IF;
	    j := j + 1;
	  END LOOP;
	  
	  IF lineage_evaluate(lineage_expr, bs) then
	    ss := 1;
	    FOREACH var in array vars LOOP
	      IF lineage_true_or_false(bs, var) then
	        ss := ss * lineage_probt(var);
	      ELSE
	        ss := ss * (1- lineage_probt(var));
	      END IF;
	    END LOOP;
	    s := s + ss;
	  END IF;
	  
	  i := i + 1;
	END LOOP;
	d := te-ts;
	s := power(s, d);
	RETURN s::numeric;
  END 
$$ language plpgsql;
COMMENT ON FUNCTION lineage_conft(text, date, date) IS 'computes p (normalized by the duration) for the given lineage';
