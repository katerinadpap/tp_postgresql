/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  transform the raw parse tree into a query tree
 *
 * For optimizable statements, we are careful to obtain a suitable lock on
 * each referenced table, and other modules of the backend preserve or
 * re-obtain these locks before depending on the results.  It is therefore
 * okay to do significant semantic analysis of these statements.  For
 * utility commands, no locks are obtained here (and if they were, we could
 * not be sure we'd still have them at execution).  Hence the general rule
 * for utility commands is to just dump them into a Query node untransformed.
 * DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are exceptions because they
 * contain optimizable statements, which we should transform.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/parser/analyze.c
 *
 *-------------------------------------------------------------------------
 */


#ifdef DEBUG
 #define DB
#else
 #define DB for(;0;)
#endif 

#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_oper.h"
#include "parser/parse_param.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/rel.h"

/* Hook for plugins to get control at end of parse analysis */
post_parse_analyze_hook_type post_parse_analyze_hook = NULL;

static Query *transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt);
static Query *transformInsertStmt(ParseState *pstate, InsertStmt *stmt);
static List *transformInsertRow(ParseState *pstate, List *exprlist, List *stmtcols, List *icolumns, List *attrnos);
static int count_rowexpr_columns(ParseState *pstate, Node *expr);
static Query *transformSelectStmt(ParseState *pstate, SelectStmt *stmt);
static Query *transformValuesClause(ParseState *pstate, SelectStmt *stmt);
static Query *transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt);
static Node *transformSetOperationTree(ParseState *pstate, SelectStmt *stmt, bool isTopLevel, List **targetlist);
static void determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist);
static Query *transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt);
static List *transformReturningList(ParseState *pstate, List *returningList);
static Query *transformDeclareCursorStmt(ParseState *pstate, DeclareCursorStmt *stmt);
static Query *transformExplainStmt(ParseState *pstate, ExplainStmt *stmt);
static Query *transformCreateTableAsStmt(ParseState *pstate, CreateTableAsStmt *stmt);
static void transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc, bool pushedDown);

/* TPDB - KATERINA */
// general functions used in all cases
static ResTarget *get_res_target(char* column_name, char* res_name);
static ResTarget * make_tag_column(char* res_name, long tag_value);
static Query *transformTimeAdjustmentStmt(ParseState *pstate, SelectStmt *stmt);
static void printTargetList(char * listname, List *resT_list);

/* TPDB - KATERINA */
// for joins
static List *getNonTPColumnNamesForAStar(ParseState *pstate, char* aliasname, Node *fromTree, bool include);
static List *getSortListForOuterJoins(List *resT_list);
static List *getNonTPColumnNamesFinalTarget(List *userTargetList, char* aliasname);
static List *getJoinOuterFacts(List *resT_list);

/* TPDB - KATERINA */
// for set operations
static List *getNonTPForSet(ParseState *pstate, char* aliasname, Node *fromTree);
static List *getSortListForSet(List *resT_list);

/*
 * parse_analyze
 *		Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.  Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 */
Query *
parse_analyze(Node *parseTree, const char *sourceText, Oid *paramTypes, int numParams) {
	ParseState *pstate = make_parsestate(NULL );
	Query *query;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;

	if (numParams > 0)
		parse_fixed_parameters(pstate, paramTypes, numParams);

	query = transformTopLevelStmt(pstate, parseTree);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook)(pstate, query);

	free_parsestate(pstate);

	return query;
}

/*
 * parse_analyze_varparams
 *
 * This variant is used when it's okay to deduce information about $n
 * symbol datatypes from context.  The passed-in paramTypes[] array can
 * be modified or enlarged (via repalloc).
 */
Query *
parse_analyze_varparams(Node *parseTree, const char *sourceText, Oid **paramTypes, int *numParams) {
	ParseState *pstate = make_parsestate(NULL );
	Query *query;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;

	parse_variable_parameters(pstate, paramTypes, numParams);

	query = transformTopLevelStmt(pstate, parseTree);

	/* make sure all is well with parameter types */
	check_variable_parameters(pstate, query);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook)(pstate, query);

	free_parsestate(pstate);

	return query;
}

/*
 * parse_sub_analyze
 *		Entry point for recursively analyzing a sub-statement.
 */
Query *
parse_sub_analyze(Node *parseTree, ParseState *parentParseState, CommonTableExpr *parentCTE, bool locked_from_parent) {
	ParseState *pstate = make_parsestate(parentParseState);
	Query *query;

	pstate->p_parent_cte = parentCTE;
	pstate->p_locked_from_parent = locked_from_parent;

	query = transformStmt(pstate, parseTree);

	free_parsestate(pstate);

	return query;
}

/*
 * transformTopLevelStmt -
 *	  transform a Parse tree into a Query tree.
 *
 * The only thing we do here that we don't do in transformStmt() is to
 * convert SELECT ... INTO into CREATE TABLE AS.  Since utility statements
 * aren't allowed within larger statements, this is only allowed at the top
 * of the parse tree, and so we only try it before entering the recursive
 * transformStmt() processing.
 */
Query *
transformTopLevelStmt(ParseState *pstate, Node *parseTree) {
	if (IsA(parseTree, SelectStmt)) {
		SelectStmt *stmt = (SelectStmt *) parseTree;

		// printf("WHEN TOP LEVEL = %d\n", stmt->lineage_type);

		/* If it's a set-operation tree, drill down to leftmost SelectStmt */
		while (stmt && stmt->op != SETOP_NONE)
			stmt = stmt->larg;
		Assert(stmt && IsA(stmt, SelectStmt) &&stmt->larg == NULL);

		if (stmt->intoClause) {
			CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);

			ctas->query = parseTree;
			ctas->into = stmt->intoClause;
			ctas->relkind = OBJECT_TABLE;
			ctas->is_select_into = true;

			/*
			 * Remove the intoClause from the SelectStmt.  This makes it safe
			 * for transformSelectStmt to complain if it finds intoClause set
			 * (implying that the INTO appeared in a disallowed place).
			 */
			stmt->intoClause = NULL;

			parseTree = (Node *) ctas;
		}
		/* TPDB - KATERINA */
		else {
			printf("\n\nTHIS IS A TOP LEVEL STATEMENT\n");
			stmt = (SelectStmt *) parseTree;
			stmt->isTopLevelStmt = true;
			parseTree = (Node *) stmt;
		}
	}

	return transformStmt(pstate, parseTree);
}

/*
 * transformStmt -
 *	  recursively transform a Parse tree into a Query tree.
 */
Query *
transformStmt(ParseState *pstate, Node *parseTree) {
	Query *result;

	switch (nodeTag(parseTree) ) {
	/*
	 * Optimizable statements
	 */
	case T_InsertStmt:
		result = transformInsertStmt(pstate, (InsertStmt *) parseTree);
		break;

	case T_DeleteStmt:
		result = transformDeleteStmt(pstate, (DeleteStmt *) parseTree);
		break;

	case T_UpdateStmt:
		result = transformUpdateStmt(pstate, (UpdateStmt *) parseTree);
		break;

	case T_SelectStmt: {
		SelectStmt *n = (SelectStmt *) parseTree;
		// printf("----------------------- START ------------------------------\n");
		// printf("BEFORE ANYTHING  - %d \n", n->dimension_type);
		// pprint(n);
		// fflush(stdout);
		// printf("------------------------ END --------------------------------\n");
		// printf("\n\n\n");

		if (n->valuesLists) {
			result = transformValuesClause(pstate, n);
		}

		/* TPDB - KATERINA */
		else if (n->timeAdjustmentOperation) {
			/* TPDB - KATERINA */
			//printf("BEFORE TIME ADJUSTMENT\n\n");
			fflush(stdout);
			result = transformTimeAdjustmentStmt(pstate, n);
			//printf("AFTER TIME ADJUSTMENT\n\n");
		}

		else if (n->op == SETOP_NONE) {
			//printf("BEFORE NO SET OP  - %d \n", n->dimension_type);
			fflush(stdout);
			result = transformSelectStmt(pstate, n);
			//printf("AFTER NO SET OP  - %d \n", n->lineage_type);
		} else {
			//printf("BEFORE SET OP  - %d \n", n->dimension_type);
			result = transformSetOperationStmt(pstate, n);
		}
	}
		break;

		/*
		 * Special cases
		 */
	case T_DeclareCursorStmt:
		result = transformDeclareCursorStmt(pstate, (DeclareCursorStmt *) parseTree);
		break;

	case T_ExplainStmt:
		result = transformExplainStmt(pstate, (ExplainStmt *) parseTree);
		break;

	case T_CreateTableAsStmt:
		result = transformCreateTableAsStmt(pstate, (CreateTableAsStmt *) parseTree);
		break;

	default:

		/*
		 * other statements don't require any transformation; just return
		 * the original parsetree with a Query node plastered on top.
		 */
		result = makeNode(Query);
		result->commandType = CMD_UTILITY;
		result->utilityStmt = (Node *) parseTree;
		break;
	}

	/* Mark as original query until we learn differently */
	result->querySource = QSRC_ORIGINAL;
	result->canSetTag = true;

	//	printf("FINAL QUERY\n")
	//	pprint(result);
	//	fflush(stdout);

	return result;
}

/*
 * analyze_requires_snapshot
 *		Returns true if a snapshot must be set before doing parse analysis
 *		on the given raw parse tree.
 *
 * Classification here should match transformStmt().
 */
bool analyze_requires_snapshot(Node *parseTree) {
	bool result;

	switch (nodeTag(parseTree) ) {
	/*
	 * Optimizable statements
	 */
	case T_InsertStmt:
	case T_DeleteStmt:
	case T_UpdateStmt:
	case T_SelectStmt:
		result = true;
		break;

		/*
		 * Special cases
		 */
	case T_DeclareCursorStmt:
		/* yes, because it's analyzed just like SELECT */
		result = true;
		break;

	case T_ExplainStmt:
	case T_CreateTableAsStmt:
		/* yes, because we must analyze the contained statement */
		result = true;
		break;

	default:
		/* other utility statements don't have any real parse analysis */
		result = false;
		break;
	}

	return result;
}

/*
 * transformDeleteStmt -
 *	  transforms a Delete Statement
 */
static Query *
transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt) {
	Query *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	Node *qual;

	qry->commandType = CMD_DELETE;

	/* process the WITH clause independently of all else */
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* set up range table with just the result rel */
	qry->resultRelation = setTargetTable(pstate, stmt->relation, interpretInhOption(stmt->relation->inhOpt), true, ACL_DELETE);

	/* grab the namespace item made by setTargetTable */
	nsitem = (ParseNamespaceItem *) llast(pstate->p_namespace);

	/* there's no DISTINCT in DELETE */
	qry->distinctClause = NIL;

	/* subqueries in USING cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * The USING clause is non-standard SQL syntax, and is equivalent in
	 * functionality to the FROM list that can be specified for UPDATE. The
	 * USING keyword is used rather than FROM because FROM is already a
	 * keyword in the DELETE syntax.
	 */
	transformFromClause(pstate, stmt->usingClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	qual = transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");

	qry->returningList = transformReturningList(pstate, stmt->returningList);

	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasAggs = pstate->p_hasAggs;
	if (pstate->p_hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformInsertStmt -
 *	  transform an Insert Statement
 */
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt) {
	Query *qry = makeNode(Query);
	SelectStmt *selectStmt = (SelectStmt *) stmt->selectStmt;
	List *exprList = NIL;
	bool isGeneralSelect;
	List *sub_rtable;
	List *sub_namespace;
	List *icolumns;
	List *attrnos;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	ListCell *icols;
	ListCell *attnos;
	ListCell *lc;

	/* There can't be any outer WITH to worry about */
	Assert(pstate->p_ctenamespace == NIL);

	qry->commandType = CMD_INSERT;
	pstate->p_is_insert = true;

	/* process the WITH clause independently of all else */
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * We have three cases to deal with: DEFAULT VALUES (selectStmt == NULL),
	 * VALUES list, or general SELECT input.  We special-case VALUES, both for
	 * efficiency and so we can handle DEFAULT specifications.
	 *
	 * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
	 * VALUES clause.  If we have any of those, treat it as a general SELECT;
	 * so it will work, but you can't use DEFAULT items together with those.
	 */
	isGeneralSelect = (selectStmt
			&& (selectStmt->valuesLists == NIL || selectStmt->sortClause != NIL || selectStmt->limitOffset != NULL
					|| selectStmt->limitCount != NULL || selectStmt->lockingClause != NIL || selectStmt->withClause != NULL ));

	/*
	 * If a non-nil rangetable/namespace was passed in, and we are doing
	 * INSERT/SELECT, arrange to pass the rangetable/namespace down to the
	 * SELECT.  This can only happen if we are inside a CREATE RULE, and in
	 * that case we want the rule's OLD and NEW rtable entries to appear as
	 * part of the SELECT's rtable, not as outer references for it.  (Kluge!)
	 * The SELECT's joinlist is not affected however.  We must do this before
	 * adding the target table to the INSERT's rtable.
	 */
	if (isGeneralSelect) {
		sub_rtable = pstate->p_rtable;
		pstate->p_rtable = NIL;
		sub_namespace = pstate->p_namespace;
		pstate->p_namespace = NIL;
	} else {
		sub_rtable = NIL; /* not used, but keep compiler quiet */
		sub_namespace = NIL;
	}

	/*
	 * Must get write lock on INSERT target table before scanning SELECT, else
	 * we will grab the wrong kind of initial lock if the target table is also
	 * mentioned in the SELECT part.  Note that the target table is not added
	 * to the joinlist or namespace.
	 */
	qry->resultRelation = setTargetTable(pstate, stmt->relation, false, false, ACL_INSERT);

	/* Validate stmt->cols list, or build default list if no list given */
	icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);
	Assert(list_length(icolumns) == list_length(attrnos));

	/*
	 * Determine which variant of INSERT we have.
	 */
	if (selectStmt == NULL ) {
		/*
		 * We have INSERT ... DEFAULT VALUES.  We can handle this case by
		 * emitting an empty targetlist --- all columns will be defaulted when
		 * the planner expands the targetlist.
		 */
		exprList = NIL;
	} else if (isGeneralSelect) {
		/*
		 * We make the sub-pstate a child of the outer pstate so that it can
		 * see any Param definitions supplied from above.  Since the outer
		 * pstate's rtable and namespace are presently empty, there are no
		 * side-effects of exposing names the sub-SELECT shouldn't be able to
		 * see.
		 */
		ParseState *sub_pstate = make_parsestate(pstate);
		Query *selectQuery;

		/*
		 * Process the source SELECT.
		 *
		 * It is important that this be handled just like a standalone SELECT;
		 * otherwise the behavior of SELECT within INSERT might be different
		 * from a stand-alone SELECT. (Indeed, Postgres up through 6.5 had
		 * bugs of just that nature...)
		 */
		sub_pstate->p_rtable = sub_rtable;
		sub_pstate->p_joinexprs = NIL; /* sub_rtable has no joins */
		sub_pstate->p_namespace = sub_namespace;

		selectQuery = transformStmt(sub_pstate, stmt->selectStmt);

		free_parsestate(sub_pstate);

		/* The grammar should have produced a SELECT */
		if (!IsA(selectQuery, Query) || selectQuery->commandType != CMD_SELECT || selectQuery->utilityStmt != NULL )
			elog(ERROR, "unexpected non-SELECT command in INSERT ... SELECT");

		/*
		 * Make the source be a subquery in the INSERT's rangetable, and add
		 * it to the INSERT's joinlist.
		 */
		rte = addRangeTableEntryForSubquery(pstate, selectQuery, makeAlias("*SELECT*", NIL ), false, false );
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

		/*----------
		 * Generate an expression list for the INSERT that selects all the
		 * non-resjunk columns from the subquery.  (INSERT's tlist must be
		 * separate from the subquery's tlist because we may add columns,
		 * insert datatype coercions, etc.)
		 *
		 * HACK: unknown-type constants and params in the SELECT's targetlist
		 * are copied up as-is rather than being referenced as subquery
		 * outputs.  This is to ensure that when we try to coerce them to
		 * the target column's datatype, the right things happen (see
		 * special cases in coerce_type).  Otherwise, this fails:
		 *		INSERT INTO foo SELECT 'bar', ... FROM baz
		 *----------
		 */
		exprList = NIL;
		foreach(lc, selectQuery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr *expr;

			if (tle->resjunk)
				continue;
			if (tle->expr && (IsA(tle->expr, Const) || IsA(tle->expr, Param))&&
			exprType((Node *) tle->expr) == UNKNOWNOID)
				expr = tle->expr;
			else {
				Var *var = makeVarFromTargetEntry(rtr->rtindex, tle);

				var->location = exprLocation((Node *) tle->expr);
				expr = (Expr *) var;
			}
			exprList = lappend(exprList, expr);
		}

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos);
	} else if (list_length(selectStmt->valuesLists) > 1) {
		/*
		 * Process INSERT ... VALUES with multiple VALUES sublists. We
		 * generate a VALUES RTE holding the transformed expression lists, and
		 * build up a targetlist containing Vars that reference the VALUES
		 * RTE.
		 */
		List *exprsLists = NIL;
		List *collations = NIL;
		int sublist_length = -1;
		bool lateral = false;
		int i;

		Assert(selectStmt->intoClause == NULL);

		foreach(lc, selectStmt->valuesLists)
		{
			List *sublist = (List *) lfirst(lc);

			/* Do basic expression transformation (same as a ROW() expr) */
			sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES);

			/*
			 * All the sublists must be the same length, *after*
			 * transformation (which might expand '*' into multiple items).
			 * The VALUES RTE can't handle anything different.
			 */
			if (sublist_length < 0) {
				/* Remember post-transformation length of first sublist */
				sublist_length = list_length(sublist);
			} else if (sublist_length != list_length(sublist)) {
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("VALUES lists must all be the same length"), parser_errposition(pstate, exprLocation((Node *) sublist))));
			}

			/* Prepare row for assignment to target table */
			sublist = transformInsertRow(pstate, sublist, stmt->cols, icolumns, attrnos);

			/*
			 * We must assign collations now because assign_query_collations
			 * doesn't process rangetable entries.  We just assign all the
			 * collations independently in each row, and don't worry about
			 * whether they are consistent vertically.  The outer INSERT query
			 * isn't going to care about the collations of the VALUES columns,
			 * so it's not worth the effort to identify a common collation for
			 * each one here.  (But note this does have one user-visible
			 * consequence: INSERT ... VALUES won't complain about conflicting
			 * explicit COLLATEs in a column, whereas the same VALUES
			 * construct in another context would complain.)
			 */
			assign_list_collations(pstate, sublist);

			exprsLists = lappend(exprsLists, sublist);
		}

		/*
		 * Although we don't really need collation info, let's just make sure
		 * we provide a correctly-sized list in the VALUES RTE.
		 */
		for (i = 0; i < sublist_length; i++)
			collations = lappend_oid(collations, InvalidOid );

		/*
		 * Ordinarily there can't be any current-level Vars in the expression
		 * lists, because the namespace was empty ... but if we're inside
		 * CREATE RULE, then NEW/OLD references might appear.  In that case we
		 * have to mark the VALUES RTE as LATERAL.
		 */
		if (list_length(pstate->p_rtable) != 1 && contain_vars_of_level((Node *) exprsLists, 0))
			lateral = true;

		/*
		 * Generate the VALUES RTE
		 */
		rte = addRangeTableEntryForValues(pstate, exprsLists, collations, NULL, lateral, true );
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);

		/*
		 * Generate list of Vars referencing the RTE
		 */
		expandRTE(rte, rtr->rtindex, 0, -1, false, NULL, &exprList);
	} else {
		/*
		 * Process INSERT ... VALUES with a single VALUES sublist.  We treat
		 * this case separately for efficiency.  The sublist is just computed
		 * directly as the Query's targetlist, with no VALUES RTE.  So it
		 * works just like a SELECT without any FROM.
		 */
		List *valuesLists = selectStmt->valuesLists;

		Assert(list_length(valuesLists) == 1);Assert(selectStmt->intoClause == NULL);

		/* Do basic expression transformation (same as a ROW() expr) */
		exprList = transformExpressionList(pstate, (List *) linitial(valuesLists), EXPR_KIND_VALUES);

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList, stmt->cols, icolumns, attrnos);
	}

	/*
	 * Generate query's target list using the computed list of expressions.
	 * Also, mark all the target columns as needing insert permissions.
	 */
	rte = pstate->p_target_rangetblentry;
	qry->targetList = NIL;
	icols = list_head(icolumns);
	attnos = list_head(attrnos);
	foreach(lc, exprList)
	{
		Expr *expr = (Expr *) lfirst(lc);
		ResTarget *col;
		AttrNumber attr_num;
		TargetEntry *tle;

		col = (ResTarget *) lfirst(icols);
		Assert(IsA(col, ResTarget));
		attr_num = (AttrNumber) lfirst_int(attnos);

		tle = makeTargetEntry(expr, attr_num, col->name, false );
		qry->targetList = lappend(qry->targetList, tle);

		rte->modifiedCols = bms_add_member(rte->modifiedCols, attr_num - FirstLowInvalidHeapAttributeNumber);

		icols = lnext(icols);
		attnos = lnext(attnos);
	}

	/*
	 * If we have a RETURNING clause, we need to add the target relation to
	 * the query namespace before processing it, so that Var references in
	 * RETURNING will work.  Also, remove any namespace entries added in a
	 * sub-SELECT or VALUES list.
	 */
	if (stmt->returningList) {
		pstate->p_namespace = NIL;
		addRTEtoQuery(pstate, pstate->p_target_rangetblentry, false, true, true );
		qry->returningList = transformReturningList(pstate, stmt->returningList);
	}

	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL );

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * Prepare an INSERT row for assignment to the target table.
 *
 * The row might be either a VALUES row, or variables referencing a
 * sub-SELECT output.
 */
static List *
transformInsertRow(ParseState *pstate, List *exprlist, List *stmtcols, List *icolumns, List *attrnos) {
	List *result;
	ListCell *lc;
	ListCell *icols;
	ListCell *attnos;

	/*
	 * Check length of expr list.  It must not have more expressions than
	 * there are target columns.  We allow fewer, but only if no explicit
	 * columns list was given (the remaining columns are implicitly
	 * defaulted).  Note we must check this *after* transformation because
	 * that could expand '*' into multiple items.
	 */
	if (list_length(exprlist) > list_length(icolumns))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("INSERT has more expressions than target columns"), parser_errposition(pstate, exprLocation(list_nth(exprlist, list_length(icolumns))))));
	if (stmtcols != NIL && list_length(exprlist) < list_length(icolumns)) {
		/*
		 * We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
		 * where the user accidentally created a RowExpr instead of separate
		 * columns.  Add a suitable hint if that seems to be the problem,
		 * because the main error message is quite misleading for this case.
		 * (If there's no stmtcols, you'll get something about data type
		 * mismatch, which is less misleading so we don't worry about giving a
		 * hint in that case.)
		 */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("INSERT has more target columns than expressions"), ((list_length(exprlist) == 1 && count_rowexpr_columns(pstate, linitial(exprlist)) == list_length(icolumns)) ? errhint("The insertion source is a row expression containing the same number of columns expected by the INSERT. Did you accidentally use extra parentheses?") : 0), parser_errposition(pstate, exprLocation(list_nth(icolumns, list_length(exprlist))))));
	}

	/*
	 * Prepare columns for assignment to target table.
	 */
	result = NIL;
	icols = list_head(icolumns);
	attnos = list_head(attrnos);
	foreach(lc, exprlist)
	{
		Expr *expr = (Expr *) lfirst(lc);
		ResTarget *col;

		col = (ResTarget *) lfirst(icols);
		Assert(IsA(col, ResTarget));

		expr = transformAssignedExpr(pstate, expr, EXPR_KIND_INSERT_TARGET, col->name, lfirst_int(attnos), col->indirection, col->location);

		result = lappend(result, expr);

		icols = lnext(icols);
		attnos = lnext(attnos);
	}

	return result;
}

/*
 * count_rowexpr_columns -
 *	  get number of columns contained in a ROW() expression;
 *	  return -1 if expression isn't a RowExpr or a Var referencing one.
 *
 * This is currently used only for hint purposes, so we aren't terribly
 * tense about recognizing all possible cases.  The Var case is interesting
 * because that's what we'll get in the INSERT ... SELECT (...) case.
 */
static int count_rowexpr_columns(ParseState *pstate, Node *expr) {
	if (expr == NULL )
		return -1;
	if (IsA(expr, RowExpr))
		return list_length(((RowExpr *) expr)->args);
	if (IsA(expr, Var)) {
		Var *var = (Var *) expr;
		AttrNumber attnum = var->varattno;

		if (attnum > 0 && var->vartype == RECORDOID) {
			RangeTblEntry *rte;

			rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
			if (rte->rtekind == RTE_SUBQUERY) {
				/* Subselect-in-FROM: examine sub-select's output expr */
				TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList, attnum);

				if (ste == NULL || ste->resjunk)
					return -1;
				expr = (Node *) ste->expr;
				if (IsA(expr, RowExpr))
					return list_length(((RowExpr *) expr)->args);
			}
		}
	}
	return -1;
}

/*
 * transformSelectStmt -
 *	  transforms a Select Statement
 *
 * Note: this covers only cases with no set operations and no VALUES lists;
 * see below for the other cases.
 */
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt) {
	Query *qry = makeNode(Query);
	Node *qual;
	ListCell *l;

	/*
	 * TPDB - KATERINA : Modify Join Statements
	 */

	int joinType = -1;
	TimeAdjustmentExpr    *timeAdjustment = makeNode(TimeAdjustmentExpr);

	if (stmt->dimension_type != DIMENSION_NONE) {

		bool isCartesian = (stmt->fromClause)->length > 1;
		bool isJoin = IsA(linitial(stmt->fromClause), JoinExpr);
		JoinExpr *jExp = makeNode(JoinExpr);
		if (isJoin || isCartesian) {
				// get the join type
				jExp = (JoinExpr *) linitial(stmt->fromClause);
				joinType = jExp->jointype;
			
			
			Node *left_norm, *right_norm;
			Alias *lalias = NULL, *ralias = NULL;
	
			// Identify the left and the right relation
			if (isCartesian) { /* CARTESIAN PRODUCT AND INNER JOIN */
				left_norm  = linitial(stmt->fromClause); // left join argument
				right_norm = lsecond(stmt->fromClause);  // right join argument
			}
			else if (isJoin) {
				left_norm  = jExp->larg; // left join argument
				right_norm = jExp->rarg; // right join argument
			}
	
			// Get the name or alias of the left relation
			if (IsA(left_norm, RangeSubselect)) {
				lalias = ((RangeSubselect *) left_norm)->alias;
			} else if (IsA(left_norm, RangeVar)) { // left argument is a subquery
				RangeVar *v = (RangeVar *) left_norm;
				if (v->alias != NULL )
					lalias = v->alias;
				else
					lalias = makeAlias(v->relname, NIL );
			}
			DB printf("lalias = %s\n", lalias->aliasname);
	
			// Get the name or alias of the right relation
			if (IsA(right_norm, RangeSubselect)) {
				ralias = ((RangeSubselect *) right_norm)->alias;
			} else if (IsA(right_norm, RangeVar)) { // left argument is a subquery
				RangeVar *v = (RangeVar *) right_norm;
				if (v->alias != NULL )
					ralias = v->alias;
				else
					ralias = makeAlias(v->relname, NIL );
			}
			DB printf("ralias = %s\n", ralias->aliasname);
	
			// Elements for the Overlapping Condition
			ColumnRef *left_ts_ref = makeNode(ColumnRef), *left_te_ref = makeNode(ColumnRef), *right_ts_ref = makeNode(ColumnRef),
   					  *right_te_ref = makeNode(ColumnRef), *union_t_ref = makeNode(ColumnRef);
	
			// Facts
			List *leftFact, *rightFact;
	
			// Lineages: lin1, lin2
			ColumnRef *lin1_column_ref = makeNode(ColumnRef); 
			ColumnRef *lin2_column_ref = makeNode(ColumnRef); 
			ResTarget *join_rt_lin1 = makeNode(ResTarget);
			ResTarget *join_rt_lin2 = makeNode(ResTarget);
	
	
			// Overlap [p1, p2]
			ResTarget *max_rt = makeNode(ResTarget);
			ResTarget *min_rt = makeNode(ResTarget);
	
	
			if (joinType == JOIN_RIGHT) {

				// Elements for the Overlapping Condition
				left_ts_ref->fields = list_make2 (makeString(ralias->aliasname), makeString("ts"));
				left_ts_ref->location = -1;
				left_te_ref->fields = list_make2(makeString(ralias->aliasname), makeString("te"));
				left_te_ref->location = -1;
				right_ts_ref->fields = list_make2(makeString(lalias->aliasname), makeString("ts"));
				right_ts_ref->location = -1;
				right_te_ref->fields = list_make2(makeString(lalias->aliasname), makeString("te"));
				right_te_ref->location = -1;
		
				// The overlapping condition
				jExp->quals = (Node *) makeA_Expr(AEXPR_AND, NIL, jExp->quals,
							  (Node *) makeSimpleA_Expr(AEXPR_OP, "<", (Node *) left_ts_ref, (Node *) right_te_ref, -1), -1);
				jExp->quals = (Node *) makeA_Expr(AEXPR_AND, NIL, jExp->quals,
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<", (Node *) right_ts_ref, (Node *) left_te_ref, -1), -1);
		

				// Overlap: [p1, p2]
				MinMaxExpr *max_ts = makeNode(MinMaxExpr);
			
				max_ts->args = list_make2(copyObject(left_ts_ref), copyObject(right_ts_ref));
				max_ts->op = IS_GREATEST;
				max_ts->location = -1;
				max_rt->name = "p1"; // renaming of s.ts to remove ambiguity
				max_rt->indirection = NIL;
				max_rt->val = (Node *) max_ts;
				max_rt->location = -1;
			
				MinMaxExpr *min_te = makeNode(MinMaxExpr);
				min_te->args = list_make2(copyObject(left_te_ref), copyObject(right_te_ref));
				min_te->op = IS_LEAST;
				min_te->location = -1;
				min_rt->name = "p2"; // renaming of s.ts to remove ambiguity
				min_rt->indirection = NIL;
				min_rt->val = (Node *) min_te;
				min_rt->location = -1;
		
		
				// Lineages: lin1, lin2
				ColumnRef *lin1_column_ref = makeNode(ColumnRef); 
			
				lin1_column_ref->fields = list_make2(makeString(ralias->aliasname), makeString("lineage"));
				lin1_column_ref->location = -1;
				join_rt_lin1->name = "lin1";
				join_rt_lin1->indirection = NIL;
				join_rt_lin1->val = (Node *) lin1_column_ref;
				join_rt_lin1->location = -1;
			
				ColumnRef *lin2_column_ref = makeNode(ColumnRef); 		
				lin2_column_ref->fields = list_make2(makeString(lalias->aliasname), makeString("lineage"));
				lin2_column_ref->location = -1;
				join_rt_lin2->name = "lin2";
				join_rt_lin2->indirection = NIL;
				join_rt_lin2->val = (Node *) lin2_column_ref;
				join_rt_lin2->location = -1;
		
			} else {
	
				// Elements for the Overlapping Condition
				left_ts_ref->fields = list_make2 (makeString(lalias->aliasname), makeString("ts"));
				left_ts_ref->location = -1;
				left_te_ref->fields = list_make2(makeString(lalias->aliasname), makeString("te"));
				left_te_ref->location = -1;
				right_ts_ref->fields = list_make2(makeString(ralias->aliasname), makeString("ts"));
				right_ts_ref->location = -1;
				right_te_ref->fields = list_make2(makeString(ralias->aliasname), makeString("te"));
				right_te_ref->location = -1;
		
				// The overlapping condition
				jExp->quals = (Node *) makeA_Expr(AEXPR_AND, NIL, jExp->quals,
							  (Node *) makeSimpleA_Expr(AEXPR_OP, "<", (Node *) left_ts_ref, (Node *) right_te_ref, -1), -1);
				jExp->quals = (Node *) makeA_Expr(AEXPR_AND, NIL, jExp->quals,
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<", (Node *) right_ts_ref, (Node *) left_te_ref, -1), -1);
		
				
				// Overlap: [p1, p2]
				MinMaxExpr *max_ts = makeNode(MinMaxExpr);
			
				max_ts->args = list_make2(copyObject(left_ts_ref), copyObject(right_ts_ref));
				max_ts->op = IS_GREATEST;
				max_ts->location = -1;
				max_rt->name = "p1"; // renaming of s.ts to remove ambiguity
				max_rt->indirection = NIL;
				max_rt->val = (Node *) max_ts;
				max_rt->location = -1;
			
				MinMaxExpr *min_te = makeNode(MinMaxExpr);
				min_te->args = list_make2(copyObject(left_te_ref), copyObject(right_te_ref));
				min_te->op = IS_LEAST;
				min_te->location = -1;
				min_rt->name = "p2"; // renaming of s.ts to remove ambiguity
				min_rt->indirection = NIL;
				min_rt->val = (Node *) min_te;
				min_rt->location = -1;
		
		
	
				// ResTarget *join_rt_lin1 = makeNode(ResTarget);
			
				lin1_column_ref->fields = list_make2(makeString(lalias->aliasname), makeString("lineage"));
				lin1_column_ref->location = -1;
				join_rt_lin1->name = "lin1";
				join_rt_lin1->indirection = NIL;
				join_rt_lin1->val = (Node *) lin1_column_ref;
				join_rt_lin1->location = -1;
			
				// ResTarget *join_rt_lin2 = makeNode(ResTarget);
			
				lin2_column_ref->fields = list_make2(makeString(ralias->aliasname), makeString("lineage"));
				lin2_column_ref->location = -1;
				join_rt_lin2->name = "lin2";
				join_rt_lin2->indirection = NIL;
				join_rt_lin2->val = (Node *) lin2_column_ref;
				join_rt_lin2->location = -1;
			}
		
			// Lineage Fact
			A_Const *emptyString = makeNode(A_Const);
			emptyString->location = -1;
			emptyString->val = *makeString("");
		
			// Function to be used for the output lineage expression
			FuncCall   *lineageFunction = makeNode(FuncCall);
			lineageFunction->agg_order = NIL;
			lineageFunction->agg_filter = NULL;
			lineageFunction->agg_within_group = false;
			lineageFunction->agg_star = false;
			lineageFunction->agg_distinct = false;
			lineageFunction->func_variadic = false;
			lineageFunction->over = NULL;
		
			ResTarget *lineageTarget = makeNode(ResTarget);
			lineageTarget->name = "lineage";
			lineageTarget->indirection = NULL;
			
		
			if (isCartesian || joinType == JOIN_INNER) {

	
				max_rt->name = "ts";
				min_rt->name = "te";
	
				lineageFunction->args = list_make2(lin1_column_ref, lin2_column_ref);
				lineageFunction->funcname = list_make1(makeString("concat_lineage_and"));	
				lineageTarget->val = (Node *) lineageFunction;
	
				ResTarget *join_rt_ts = makeNode(ResTarget);
				join_rt_ts->name = "ts";
				join_rt_ts->indirection = NIL;
				join_rt_ts->val = (Node *) left_ts_ref;
				join_rt_ts->location = -1;
	
				ResTarget *join_rt_te = makeNode(ResTarget);
				join_rt_te->name = "te";
				join_rt_te->indirection = NIL;
				join_rt_te->val = (Node *) left_te_ref;
				join_rt_te->location = -1;
		
				stmt->targetList = list_concat(list_copy(leftFact),list_copy(rightFact));
				stmt->targetList = list_concat(stmt->targetList, list_make2 (max_rt,min_rt));	
				stmt->targetList = lappend(stmt->targetList, lineageTarget);
	
				jExp->jointype = JOIN_INNER;
				stmt->fromClause = list_make1(jExp);
	
				printf("END OF ANALYZE\n");	
	
			} else if (joinType == JOIN_LEFT) {
				
				// Fact renaming takes place to resolve conflicts occuring when attributes of the input relations have
				// the same name: e.g. u(a,b) and v(a,b) are renamed to u_a, u_b, v_a, v_b
				leftFact = getNonTPColumnNamesForAStar(make_parsestate(pstate), lalias->aliasname, copyObject(left_norm), false);
				rightFact = getNonTPColumnNamesForAStar(make_parsestate(pstate), ralias->aliasname, copyObject(right_norm), false);
	
				// Ts argument of the join
				ResTarget *join_rt_ts = makeNode(ResTarget);
				join_rt_ts->name = "ts";
				join_rt_ts->indirection = NIL;
				join_rt_ts->val = (Node *) left_ts_ref;
				join_rt_ts->location = -1;
	
				// Te argument of the join
				ResTarget *join_rt_te = makeNode(ResTarget);
				join_rt_te->name = "te";
				join_rt_te->indirection = NIL;
				join_rt_te->val = (Node *) left_te_ref;
				join_rt_te->location = -1;
		
				// ************************ Subrange for overlapping and unmatched windows ************************
				TimeAdjustmentExpr    *overlapping_adjustment = makeNode(TimeAdjustmentExpr);
        		overlapping_adjustment->timeAdjustmentType = 2; // Adjustment process that will compute unmatched and overlapping windows 
        		overlapping_adjustment->usingClause = NIL;
        		overlapping_adjustment->quals = NIL;
        		overlapping_adjustment->numOfLeftCols = leftFact->length;
        		overlapping_adjustment->numOfRightCols = rightFact->length;
	
				SelectStmt *overlapping_unmatched_Select = makeNode(SelectStmt);
				overlapping_unmatched_Select->fromClause = list_make1(jExp);
				overlapping_unmatched_Select->distinctClause = NIL;
				overlapping_unmatched_Select->lineage_type = LINEAGE_NONE;
				overlapping_unmatched_Select->targetList = NIL;
				overlapping_unmatched_Select->sortClause = NIL;
				overlapping_unmatched_Select->timeAdjustmentOperation = (Node *) overlapping_adjustment;
	
				overlapping_unmatched_Select->targetList = list_concat(list_copy(leftFact),list_copy(rightFact));
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (join_rt_ts,join_rt_te));	
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (join_rt_lin1,join_rt_lin2));
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (max_rt,min_rt));	
				DB printTargetList("overlapping_unmatched_Select",overlapping_unmatched_Select->targetList);
	
				// Sort the result of the join based on the facts of the left relation, the initial interval and the overlapping interval
				List *list_for_sorting = list_concat(list_copy(leftFact), list_make2 (join_rt_ts,join_rt_te));
				list_for_sorting = list_concat(list_for_sorting, list_make2 (max_rt,min_rt));	
				overlapping_unmatched_Select->sortClause = getSortListForOuterJoins(list_for_sorting);
	
	
				RangeSubselect *subrange_overlapping_unmatched = makeNode(RangeSubselect);
				subrange_overlapping_unmatched->alias = makeAlias("overlapping_unmatched", NIL);
				subrange_overlapping_unmatched->subquery = (Node *) overlapping_unmatched_Select;
	
				// ******************** Subrange that removes p1 and p2 from the window schema ********************
				SelectStmt *overlapping_unmatched_outer_Select = makeNode(SelectStmt);
				overlapping_unmatched_outer_Select->fromClause = list_make1(subrange_overlapping_unmatched);
				overlapping_unmatched_outer_Select->distinctClause = NIL;
				overlapping_unmatched_outer_Select->lineage_type = LINEAGE_NONE;
				overlapping_unmatched_outer_Select->sortClause = NIL;
				overlapping_unmatched_outer_Select->targetList = NIL;
				overlapping_unmatched_outer_Select->targetList = getJoinOuterFacts(overlapping_unmatched_Select->targetList);
				printTargetList("overlapping_unmatched_windows_Select",overlapping_unmatched_outer_Select->targetList);

				RangeSubselect *outer_subrange_overlapping_unmatched = makeNode(RangeSubselect);
				outer_subrange_overlapping_unmatched->alias = makeAlias("overlapping_unmatched_windows", NIL);
				outer_subrange_overlapping_unmatched->subquery = (Node *) overlapping_unmatched_outer_Select;
	
	
				// ************************ Subrange for negating windows ************************
				ColumnRef *cr_Astar = makeNode(ColumnRef);
				cr_Astar->fields = list_make1(makeNode(A_Star));
		
				ResTarget *rt_Astar = makeNode(ResTarget);
				rt_Astar->name = NIL;
				rt_Astar->indirection = NIL;
				rt_Astar->val = (Node *) cr_Astar;
	
	       		timeAdjustment->timeAdjustmentType = 6;  // Adjustment process that will compute negating windows 
	       		timeAdjustment->usingClause = NIL;
        		timeAdjustment->quals = NIL;
        		timeAdjustment->numOfLeftCols = leftFact->length;
        		timeAdjustment->numOfRightCols = rightFact->length;
	
				SelectStmt *negating_Select = makeNode(SelectStmt);
				negating_Select->fromClause = list_make1(outer_subrange_overlapping_unmatched);
				negating_Select->distinctClause = NIL;
				negating_Select->lineage_type = LINEAGE_NONE;
				negating_Select->targetList = NIL;
				negating_Select->sortClause = NIL;
				negating_Select->timeAdjustmentOperation = (Node *) timeAdjustment;
				negating_Select->targetList = list_make1(rt_Astar); 
	
	
				RangeSubselect *negating_subrange = makeNode(RangeSubselect);
				negating_subrange->alias = makeAlias("negating", NIL);
				negating_subrange->subquery = (Node *) negating_Select; // Add the Select stmt in a RangeSubselect
		
				
				// ******************************* Finalizing the select stmt *********************************
				list_free(stmt->fromClause);
	
				stmt->fromClause = list_make1(negating_subrange);

				// Lineages: lin1, lin2
				ColumnRef *newLin1_col = makeNode(ColumnRef); 
				newLin1_col->fields = list_make1(makeString("lin1"));
				newLin1_col->location = -1;
			
				ColumnRef *newLin2_col = makeNode(ColumnRef); 
				newLin2_col->fields = list_make1(makeString("lin2"));
				newLin2_col->location = -1;
	
	
				// Randomly get the first attribute of the leftFact using it as a criteria to distinguish
				// overlapping and negating windows whose lin1 and lin2 are both non-null
				ListCell *lc;
				ResTarget *rt_tmp;
				ColumnRef *newAtt = makeNode(ColumnRef); 
				foreach(lc, rightFact) {
					rt_tmp = (ResTarget *) lfirst(lc);
					char *l_colname = (char *) rt_tmp->name;
					newAtt->fields = list_make1(makeString(l_colname));
					newAtt->location = -1;
					break;
				}
	
	
				stmt->whereClause = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", (Node *) newLin1_col, (Node *) emptyString, -1); 
	
				// Left and Right Facts have been renamed so that the facts of each relation can be identified. At this stage
				// we revert them back to their initial names and added to the final targetlist
				List *renamingFactsLeft = getNonTPColumnNamesFinalTarget(leftFact, negating_subrange->alias->aliasname);
				DB printf("Left Facts Renamed\n");
				List *renamingFactsRight = getNonTPColumnNamesFinalTarget(rightFact, negating_subrange->alias->aliasname);
				DB printf("Right Facts Renamed\n");
				stmt->targetList = list_concat(list_copy(renamingFactsLeft), renamingFactsRight);

	
				// Attribute Ts is added to the final target list
				ColumnRef *colRef_ts = makeNode(ColumnRef);
				colRef_ts->fields = list_make2 (makeString(negating_subrange->alias->aliasname), makeString("ts"));
				colRef_ts->location = -1;
	
				ResTarget *rt_ts = makeNode(ResTarget);
				rt_ts->name = "ts";
				rt_ts->indirection = NIL;
				rt_ts->val = (Node *) colRef_ts;
				rt_ts->location = -1;
	
				stmt->targetList = lappend(stmt->targetList, rt_ts);

				// Attribute Te is added to the final target list
				ColumnRef *colRef_te = makeNode(ColumnRef);
				colRef_te->fields = list_make2 (makeString(negating_subrange->alias->aliasname), makeString("te"));
				colRef_te->location = -1;
	
				ResTarget *rt_te = makeNode(ResTarget);
				rt_te->name = "te";
				rt_te->indirection = NIL;
				rt_te->val = (Node *) colRef_te;
				rt_te->location = -1;
	
				stmt->targetList = lappend(stmt->targetList, rt_te);
	
				// Lineage Function operating on the chosen attribute (to be checked for being null) as well
				// as on lin1 and lin2, so as to form final lineage expression
				lineageFunction->args = list_make3(newLin1_col, newLin2_col, newAtt);
				lineageFunction->funcname = list_make1(makeString("lineage_for_windows"));	
				lineageTarget->val = (Node *) lineageFunction;
	
				stmt->targetList = list_concat(stmt->targetList, list_make1(lineageTarget));
				DB printTargetList("stmt_Select",stmt->targetList);
				DB printTargetList("left_fact_test",leftFact);
				DB printTargetList("right_fact_test",rightFact);

			} else {
	
				// Fact renaming takes place to resolve conflicts occuring when attributes of the input relations have
				// the same name: e.g. u(a,b) and v(a,b) are renamed to u_a, u_b, v_a, v_b
				leftFact = getNonTPColumnNamesForAStar(make_parsestate(pstate), lalias->aliasname, copyObject(left_norm), false);
				rightFact = getNonTPColumnNamesForAStar(make_parsestate(pstate), ralias->aliasname, copyObject(right_norm), false);

	
				// Ts argument of the join
				ResTarget *join_rt_ts = makeNode(ResTarget);
				join_rt_ts->name = "ts";
				join_rt_ts->indirection = NIL;
				join_rt_ts->val = (Node *) left_ts_ref;
				join_rt_ts->location = -1;
	
				// Te argument of the join
				ResTarget *join_rt_te = makeNode(ResTarget);
				join_rt_te->name = "te";
				join_rt_te->indirection = NIL;
				join_rt_te->val = (Node *) left_te_ref;
				join_rt_te->location = -1;
		
				// ************************ Subrange for overlapping and unmatched windows ************************
				TimeAdjustmentExpr    *overlapping_adjustment = makeNode(TimeAdjustmentExpr);
        		overlapping_adjustment->timeAdjustmentType = 2; // Adjustment process that will compute unmatched and overlapping windows 
        		overlapping_adjustment->usingClause = NIL;
        		overlapping_adjustment->quals = NIL;
        		overlapping_adjustment->numOfLeftCols = rightFact->length;  // again right facts proceed the left
        		overlapping_adjustment->numOfRightCols = leftFact->length;
	
				SelectStmt *overlapping_unmatched_Select = makeNode(SelectStmt);
				overlapping_unmatched_Select->fromClause = list_make1(jExp);
				overlapping_unmatched_Select->distinctClause = NIL;
				overlapping_unmatched_Select->lineage_type = LINEAGE_NONE;
				overlapping_unmatched_Select->targetList = NIL;
				overlapping_unmatched_Select->sortClause = NIL;
				overlapping_unmatched_Select->timeAdjustmentOperation = (Node *) overlapping_adjustment;
	
	
				// RightFact should proceed the leftFact so that the same algorithm with the left join can be applied
				overlapping_unmatched_Select->targetList = list_concat(list_copy(rightFact), list_copy(leftFact));
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (join_rt_ts,join_rt_te));	
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (join_rt_lin1,join_rt_lin2));
				overlapping_unmatched_Select->targetList = list_concat(overlapping_unmatched_Select->targetList, list_make2 (max_rt,min_rt));	
				DB printTargetList("overlapping_unmatched_Select",overlapping_unmatched_Select->targetList);


				// Sorting based on the rightFact is used so that the same algorithm with the left join can be applied	
				List *list_for_sorting = list_concat(list_copy(rightFact), list_make2 (join_rt_ts,join_rt_te));
				list_for_sorting = list_concat(list_for_sorting, list_make2 (max_rt,min_rt));	
				overlapping_unmatched_Select->sortClause = getSortListForOuterJoins(list_for_sorting);
	
	
				RangeSubselect *subrange_overlapping_unmatched = makeNode(RangeSubselect);
				subrange_overlapping_unmatched->alias = makeAlias("overlapping_unmatched", NIL);
				subrange_overlapping_unmatched->subquery = (Node *) overlapping_unmatched_Select;
	
				// ******************** Subrange that removes p1 and p2 from the window schema ********************
		
				SelectStmt *overlapping_unmatched_outer_Select = makeNode(SelectStmt);
				overlapping_unmatched_outer_Select->fromClause = list_make1(subrange_overlapping_unmatched);
				overlapping_unmatched_outer_Select->distinctClause = NIL;
				overlapping_unmatched_outer_Select->lineage_type = LINEAGE_NONE;
				overlapping_unmatched_outer_Select->sortClause = NIL;
				overlapping_unmatched_outer_Select->targetList = NIL;
				overlapping_unmatched_outer_Select->targetList = getJoinOuterFacts(overlapping_unmatched_Select->targetList);
				DB printTargetList("overlapping_unmatched_windows_Select",overlapping_unmatched_outer_Select->targetList);

				RangeSubselect *outer_subrange_overlapping_unmatched = makeNode(RangeSubselect);
				outer_subrange_overlapping_unmatched->alias = makeAlias("overlapping_unmatched_windows", NIL);
				outer_subrange_overlapping_unmatched->subquery = (Node *) overlapping_unmatched_outer_Select;
	
	
				// ******************************* Subrange for negating windows *********************************
				ColumnRef *cr_Astar = makeNode(ColumnRef); 	
				cr_Astar->fields = list_make1(makeNode(A_Star));
		
				ResTarget *rt_Astar = makeNode(ResTarget);
				rt_Astar->name = NIL;
				rt_Astar->indirection = NIL;
				rt_Astar->val = (Node *) cr_Astar;
	
	       		timeAdjustment->timeAdjustmentType = 6;  // Adjustment process that will compute negating windows
	       		timeAdjustment->usingClause = NIL;
        		timeAdjustment->quals = NIL;
        		timeAdjustment->numOfLeftCols = rightFact->length; // again right facts proceed the left
        		timeAdjustment->numOfRightCols = leftFact->length;
	
				SelectStmt *negating_Select = makeNode(SelectStmt);
				negating_Select->fromClause = list_make1(outer_subrange_overlapping_unmatched);
				negating_Select->distinctClause = NIL;
				negating_Select->lineage_type = LINEAGE_NONE;
				negating_Select->targetList = NIL;
				negating_Select->sortClause = NIL;
				negating_Select->timeAdjustmentOperation = (Node *) timeAdjustment;
				negating_Select->targetList = list_make1(rt_Astar); 
	
	
				RangeSubselect *negating_subrange = makeNode(RangeSubselect);
				negating_subrange->alias = makeAlias("negating", NIL);
				negating_subrange->subquery = (Node *) negating_Select; 
		
	
				// ******************************* Finalizing the select stmt *********************************
				list_free(stmt->fromClause);
	
	
				stmt->fromClause = list_make1(negating_subrange);

				// Lineages: lin1, lin2
				ColumnRef *newLin1_col = makeNode(ColumnRef); 
				newLin1_col->fields = list_make1(makeString("lin1"));
				newLin1_col->location = -1;
			
				ColumnRef *newLin2_col = makeNode(ColumnRef); 
				newLin2_col->fields = list_make1(makeString("lin2"));
				newLin2_col->location = -1;
	
				// Randomly get the first attribute of the rightFact using it as a criteria to distinguish
				// overlapping and negating windows whose lin1 and lin2 are both non-null
				ListCell *lc;
				ResTarget *rt_tmp;
				ColumnRef *newAtt = makeNode(ColumnRef); 
				foreach(lc, rightFact) {
					rt_tmp = (ResTarget *) lfirst(lc);
					char *l_colname = (char *) rt_tmp->name;
					printf("NULL_ATTRIBUTE = %s\n", l_colname );
					// newAtt = (ColumnRef *) rt_tmp->val;
					newAtt->fields = list_make1(makeString(l_colname));
					newAtt->location = -1;
					break;
				}
	
			
				stmt->whereClause = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", (Node *) newLin1_col, (Node *) emptyString, -1); 
	

				// Left and Right Facts have been renamed so that the facts of each relation can be identified. At this stage
				// we revert them back to their initial names and added to the final targetlist
				List *renamingFactsLeft = getNonTPColumnNamesFinalTarget(leftFact, negating_subrange->alias->aliasname);
				DB printf("Left Facts Renamed\n");
				List *renamingFactsRight = getNonTPColumnNamesFinalTarget(rightFact, negating_subrange->alias->aliasname);
				DB printf("Right Facts Renamed\n");
				stmt->targetList = list_concat(list_copy(renamingFactsLeft), renamingFactsRight);

	
				// Attribute Ts is added to the final target list
				ColumnRef *colRef_ts = makeNode(ColumnRef);
				colRef_ts->fields = list_make2 (makeString(negating_subrange->alias->aliasname), makeString("ts"));
				colRef_ts->location = -1;
	
				ResTarget *rt_ts = makeNode(ResTarget);
				rt_ts->name = "ts";
				rt_ts->indirection = NIL;
				rt_ts->val = (Node *) colRef_ts;
				rt_ts->location = -1;
	
				stmt->targetList = lappend(stmt->targetList, rt_ts);
	
				// Attribute Te is added to the final target list
				ColumnRef *colRef_te = makeNode(ColumnRef);
				colRef_te->fields = list_make2 (makeString(negating_subrange->alias->aliasname), makeString("te"));
				colRef_te->location = -1;
	
				ResTarget *rt_te = makeNode(ResTarget);
				rt_te->name = "te";
				rt_te->indirection = NIL;
				rt_te->val = (Node *) colRef_te;
				rt_te->location = -1;
	
				stmt->targetList = lappend(stmt->targetList, rt_te);
	
				// Lineage Function operating on the chosen attribute (to be checked for being null) as well
				// as on lin1 and lin2, so as to form final lineage expression

				lineageFunction->args = list_make3(newLin1_col, newLin2_col, newAtt);
				lineageFunction->funcname = list_make1(makeString("lineage_for_windows"));	
				lineageTarget->val = (Node *) lineageFunction;

				stmt->targetList = list_concat(stmt->targetList, list_make1(lineageTarget));
	
				DB printTargetList("stmt_Select",stmt->targetList);
				DB printTargetList("left_fact_test",leftFact);
				DB printTargetList("right_fact_test",rightFact);

			} 
		} // JOIN_IF
	} // DIMENSION_IF


		// printf(" *********** SUBSELECT AGAIN ********* \n");
		// pprint(stmt);

	qry->commandType = CMD_SELECT;

	/* process the WITH clause independently of all else */
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* Complain if we get called from someplace where INTO is not allowed */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("SELECT ... INTO is not allowed here"), parser_errposition(pstate, exprLocation((Node *) stmt->intoClause))));

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = stmt->lockingClause;

	/* make WINDOW info available for window functions, too */
	pstate->p_windowdefs = stmt->windowClause;

	printf("TRANSFORMING FROM CLAUSE - SELECT\n");

	/* process the FROM clause */
	transformFromClause(pstate, stmt->fromClause);

	printf("FROM CLAUSE DONE - SELECT\n");

	/* transform targetlist */
	printf("TRANSFORMING TARGET CLAUSE - SELECT\n");
	
	if (stmt->isTopLevelStmt) {
		printf("\n\nTHIS IS A TOP LEVEL STATEMENT\n");
	}
	printTargetList("stmt_Select",stmt->targetList);


	qry->targetList = transformTargetList(pstate, stmt->targetList, EXPR_KIND_SELECT_TARGET);

	printf("TARGET LIST DONE - SELECT\n");


	/* mark column origins */
	markTargetListOrigins(pstate, qry->targetList);

	/* transform WHERE */
	qual = transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = transformWhereClause(pstate, stmt->havingClause, EXPR_KIND_HAVING, "HAVING");

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = transformSortClause(pstate, stmt->sortClause, &qry->targetList, EXPR_KIND_ORDER_BY, true /* fix unknowns */,
			false /* allow SQL92 rules */);

	qry->groupClause = transformGroupClause(pstate, stmt->groupClause, &qry->targetList, qry->sortClause, EXPR_KIND_GROUP_BY,
			false /* allow SQL92 rules */);

	if (stmt->distinctClause == NIL ) {
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	} else if (linitial(stmt->distinctClause) == NULL ) {
		/* We had SELECT DISTINCT */
		qry->distinctClause = transformDistinctClause(pstate, &qry->targetList, qry->sortClause, false );
		qry->hasDistinctOn = false;
	} else {
		/* We had SELECT DISTINCT ON */
		qry->distinctClause = transformDistinctOnClause(pstate, stmt->distinctClause, &qry->targetList, qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset, EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount, EXPR_KIND_LIMIT, "LIMIT");

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasAggs = pstate->p_hasAggs;
	if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	foreach(l, stmt->lockingClause)
	{
		transformLockingClause(pstate, qry, (LockingClause *) lfirst(l), false );
	}

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformValuesClause -
 *	  transforms a VALUES clause that's being used as a standalone SELECT
 *
 * We build a Query containing a VALUES RTE, rather as if one had written
 *			SELECT * FROM (VALUES ...) AS "*VALUES*"
 */
static Query *
transformValuesClause(ParseState *pstate, SelectStmt *stmt) {
	Query *qry = makeNode(Query);
	List *exprsLists;
	List *collations;
	List **colexprs = NULL;
	int sublist_length = -1;
	bool lateral = false;
	RangeTblEntry *rte;
	int rtindex;
	ListCell *lc;
	ListCell *lc2;
	int i;

	qry->commandType = CMD_SELECT;

	/* Most SELECT stuff doesn't apply in a VALUES clause */
	Assert(stmt->distinctClause == NIL);Assert(stmt->intoClause == NULL);Assert(stmt->targetList == NIL);Assert(stmt->fromClause == NIL);Assert(stmt->whereClause == NULL);Assert(stmt->groupClause == NIL);Assert(stmt->havingClause == NULL);Assert(stmt->windowClause == NIL);Assert(stmt->op == SETOP_NONE);

	/* process the WITH clause independently of all else */
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * For each row of VALUES, transform the raw expressions.  This is also a
	 * handy place to reject DEFAULT nodes, which the grammar allows for
	 * simplicity.
	 *
	 * Note that the intermediate representation we build is column-organized
	 * not row-organized.  That simplifies the type and collation processing
	 * below.
	 */foreach(lc, stmt->valuesLists)
	{
		List *sublist = (List *) lfirst(lc);

		/* Do basic expression transformation (same as a ROW() expr) */
		sublist = transformExpressionList(pstate, sublist, EXPR_KIND_VALUES);

		/*
		 * All the sublists must be the same length, *after* transformation
		 * (which might expand '*' into multiple items).  The VALUES RTE can't
		 * handle anything different.
		 */
		if (sublist_length < 0) {
			/* Remember post-transformation length of first sublist */
			sublist_length = list_length(sublist);
			/* and allocate array for per-column lists */
			colexprs = (List **) palloc0(sublist_length * sizeof(List *));
		} else if (sublist_length != list_length(sublist)) {
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("VALUES lists must all be the same length"), parser_errposition(pstate, exprLocation((Node *) sublist))));
		}

		/* Check for DEFAULT and build per-column expression lists */
		i = 0;
		foreach(lc2, sublist)
		{
			Node *col = (Node *) lfirst(lc2);

			if (IsA(col, SetToDefault))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), errmsg("DEFAULT can only appear in a VALUES list within INSERT"), parser_errposition(pstate, exprLocation(col))));
			colexprs[i] = lappend(colexprs[i], col);
			i++;
		}

		/* Release sub-list's cells to save memory */
		list_free(sublist);
	}

	/*
	 * Now resolve the common types of the columns, and coerce everything to
	 * those types.  Then identify the common collation, if any, of each
	 * column.
	 *
	 * We must do collation processing now because (1) assign_query_collations
	 * doesn't process rangetable entries, and (2) we need to label the VALUES
	 * RTE with column collations for use in the outer query.  We don't
	 * consider conflict of implicit collations to be an error here; instead
	 * the column will just show InvalidOid as its collation, and you'll get a
	 * failure later if that results in failure to resolve a collation.
	 *
	 * Note we modify the per-column expression lists in-place.
	 */
	collations = NIL;
	for (i = 0; i < sublist_length; i++) {
		Oid coltype;
		Oid colcoll;

		coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL );

		foreach(lc, colexprs[i])
		{
			Node *col = (Node *) lfirst(lc);

			col = coerce_to_common_type(pstate, col, coltype, "VALUES");
			lfirst(lc) = (void *) col;
		}

		colcoll = select_common_collation(pstate, colexprs[i], true );

		collations = lappend_oid(collations, colcoll);
	}

	/*
	 * Finally, rearrange the coerced expressions into row-organized lists.
	 */
	exprsLists = NIL;
	foreach(lc, colexprs[0])
	{
		Node *col = (Node *) lfirst(lc);
		List *sublist;

		sublist = list_make1(col);
		exprsLists = lappend(exprsLists, sublist);
	}
	list_free(colexprs[0]);
	for (i = 1; i < sublist_length; i++) {
		forboth(lc, colexprs[i], lc2, exprsLists)
		{
			Node *col = (Node *) lfirst(lc);
			List *sublist = lfirst(lc2);

			/* sublist pointer in exprsLists won't need adjustment */
			(void) lappend(sublist, col);
		}
		list_free(colexprs[i]);
	}

	/*
	 * Ordinarily there can't be any current-level Vars in the expression
	 * lists, because the namespace was empty ... but if we're inside CREATE
	 * RULE, then NEW/OLD references might appear.  In that case we have to
	 * mark the VALUES RTE as LATERAL.
	 */
	if (pstate->p_rtable != NIL && contain_vars_of_level((Node *) exprsLists, 0))
		lateral = true;

	/*
	 * Generate the VALUES RTE
	 */
	rte = addRangeTableEntryForValues(pstate, exprsLists, collations, NULL, lateral, true );
	addRTEtoQuery(pstate, rte, true, true, true );

	/* assume new rte is at end */
	rtindex = list_length(pstate->p_rtable);
	Assert(rte == rt_fetch(rtindex, pstate->p_rtable));

	/*
	 * Generate a targetlist as though expanding "*"
	 */
	Assert(pstate->p_next_resno == 1);
	qry->targetList = expandRelAttrs(pstate, rte, rtindex, 0, -1);

	/*
	 * The grammar allows attaching ORDER BY, LIMIT, and FOR UPDATE to a
	 * VALUES, so cope.
	 */
	qry->sortClause = transformSortClause(pstate, stmt->sortClause, &qry->targetList, EXPR_KIND_ORDER_BY, true /* fix unknowns */,
			false /* allow SQL92 rules */);

	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset, EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount, EXPR_KIND_LIMIT, "LIMIT");

	if (stmt->lockingClause)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s cannot be applied to VALUES", LCS_asString(((LockingClause *) linitial(stmt->lockingClause))->strength))));

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL );

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/* TPDB
 * transformTimeAdjustmentStmt -
 *    transforms an TimeIntervalAdjustment Statement
 */
static Query *
transformTimeAdjustmentStmt(ParseState *pstate, SelectStmt *stmt) {
	Query *qry = makeNode(Query);
	Node *qual;
	ListCell *l;

	// TPDB - KATERINA
	TimeAdjustmentExpr *timeAdjustment = (TimeAdjustmentExpr *) stmt->timeAdjustmentOperation;

	/* TPDB - KATERINA */
	qry->commandType = CMD_SELECT;

	// process the WITH clause independently of all else
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	// Complain if we get called from someplace where INTO is not allowed
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("SELECT ... INTO is not allowed here"), parser_errposition(pstate, exprLocation((Node *) stmt->intoClause))));

	// make FOR UPDATE/FOR SHARE info available to addRangeTableEntry
	pstate->p_locking_clause = stmt->lockingClause;

	// make WINDOW info available for window functions, too
	pstate->p_windowdefs = stmt->windowClause;

	//TPDB - KATERINA
	if (timeAdjustment->timeAdjustmentType == 1) { // NORMALIZATION 

	}

	else if (timeAdjustment->timeAdjustmentType == 2) { // ALIGN


	}

	else if (timeAdjustment->timeAdjustmentType == 3) { // NORMALIZATION ON THETA

	}

	else if (timeAdjustment->timeAdjustmentType == 4) {

		DB printf(" ------------------------------------- timeAdjustmentType = 4 --------------------------------------- \n\n");

		// Get the relations involved in the normalization (they can be subqueries)
		Node *left_norm, *right_norm;
		left_norm = stmt->larg;   // left set-operation argument
		right_norm = stmt->rarg;  // right set-operation argument

		// If left_norm is a subquery, keep their aliases for the union and
		// push the lineage_type from the initial subselect
		Alias *lalias = NULL;
		if (IsA(left_norm, RangeSubselect)) {
			lalias = ((RangeSubselect *) left_norm)->alias;
			if (stmt->lineage_type != LINEAGE_NONE && IsA(((RangeSubselect *) left_norm)->subquery, SelectStmt)) {
				((SelectStmt *) ((RangeSubselect *) left_norm)->subquery)->lineage_type = stmt->lineage_type;
			}
		}
		else if (IsA(left_norm, RangeVar)) { // left argument is a subquery
			RangeVar *v = (RangeVar *) left_norm;
			if (v->alias != NULL )
				lalias = v->alias;
			else
				lalias = makeAlias(v->relname, NIL );
		}
		else if (IsA(left_norm, SelectStmt)) {
			
			Node * fromQuery = ((SelectStmt *) left_norm)->fromClause;

			if (IsA(linitial(fromQuery), RangeSubselect)) {
				lalias = ((RangeSubselect *) fromQuery)->alias;
			} else {
				RangeVar *v = (RangeVar *) linitial(fromQuery);
				if (v->alias != NULL )
					lalias = v->alias;
				else
					lalias = makeAlias(v->relname, NIL );
			}
		}
		DB printf("lalias = %s\n", lalias->aliasname);


		// If left_norm is a subquery, keep their aliases for the union and
		// push the lineage_type from the initial subselect
		Alias *ralias = NULL;
		if (IsA(right_norm, RangeSubselect)) {
			ralias = ((RangeSubselect *) right_norm)->alias;
			if (stmt->lineage_type != LINEAGE_NONE && IsA(((RangeSubselect *) right_norm)->subquery, SelectStmt)) {
				((SelectStmt *) ((RangeSubselect *) right_norm)->subquery)->lineage_type = stmt->lineage_type;
			}
		}
		else if (IsA(right_norm, RangeVar)) { // left argument is a subquery
			RangeVar *v = (RangeVar *) right_norm;
			if (v->alias != NULL )
				ralias = v->alias;
			else
				ralias = makeAlias(v->relname, NIL );
		}
		else if (IsA(right_norm, SelectStmt)) {
			
			Node * fromQuery = ((SelectStmt *) right_norm)->fromClause;

			if (IsA(linitial(fromQuery), RangeSubselect)) {
				// Node * fromQuery = linitial(tempSelect->fromClause);
				ralias = ((RangeSubselect *) linitial(fromQuery))->alias;
			} 
			else { // left argument is a subquery
				RangeVar *v = (RangeVar *) linitial(fromQuery);
				if (v->alias != NULL )
					ralias = v->alias;
				else
					ralias = makeAlias(v->relname, NIL );
			}
		}
		DB printf("ralias = %s\n", ralias->aliasname);


		// --------------------- Left Relation Prepared for the Union ---------------------
		// The attributes in the targetlist of left_union should be the ones in the
		// using attribute of the timeAdjustmentExpression 
		SelectStmt *left_union = makeNode(SelectStmt);
		left_union = left_norm;

		List *leftFact = getNonTPForSet(make_parsestate(pstate), lalias->aliasname, linitial(((SelectStmt *) left_norm)->fromClause));
		left_union->targetList = NIL;
		left_union->targetList = list_copy(leftFact);
		left_union->targetList = lappend(left_union->targetList, make_tag_column("tag", 0));
		left_union->targetList = lappend(left_union->targetList, get_res_target("ts", "ts" ));
		left_union->targetList = lappend(left_union->targetList, get_res_target("te", "te" ));
		left_union->targetList = lappend(left_union->targetList, get_res_target("lineage", "lin1"));
		left_union->targetList = lappend(left_union->targetList, get_res_target("lineage", "lin2"));
		left_union->targetList = lappend(left_union->targetList, get_res_target("ts", "p1"));
		left_union->targetList = lappend(left_union->targetList, get_res_target("te", "p2"));

		// --------------------- Right Relation Prepared for the Union ---------------------
		SelectStmt *right_union = makeNode(SelectStmt);
		right_union = right_norm;

		List *rightFact = getNonTPForSet(make_parsestate(pstate), ralias->aliasname, linitial(((SelectStmt *) right_norm)->fromClause));
		right_union->targetList = NIL;
		right_union->targetList = list_copy(rightFact);
		right_union->targetList = lappend(right_union->targetList, make_tag_column("tag", 1));
		right_union->targetList = lappend(right_union->targetList, get_res_target("ts", NULL ));
		right_union->targetList = lappend(right_union->targetList, get_res_target("te", NULL ));
		right_union->targetList = lappend(right_union->targetList, get_res_target("lineage", "lin1"));
		right_union->targetList = lappend(right_union->targetList, get_res_target("lineage", "lin2"));
		right_union->targetList = lappend(right_union->targetList, get_res_target("ts", "p1"));
		right_union->targetList = lappend(right_union->targetList, get_res_target("te", "p2"));


		// --------------------- Creation of the Union Node ---------------------
		SelectStmt *unionStmt = makeNode(SelectStmt);
		unionStmt->op = SETOP_UNION;
		unionStmt->all = TRUE;
		unionStmt->larg = (SelectStmt *) left_union;
		unionStmt->rarg = (SelectStmt *) right_union;
		 unionStmt->fromClause = NIL;
		unionStmt->timeAdjustmentOperation = NIL;

		// Add the Select stmt in a RangeSubselect
		RangeSubselect *subrange = makeNode(RangeSubselect);
		subrange->subquery = (Node *) unionStmt;
		subrange->alias = makeAlias("UNION", NIL );


		// --------------------------- Sorted Union -----------------------------
		ColumnRef *cr_Astar = makeNode(ColumnRef); 
		cr_Astar->fields = list_make1(makeNode(A_Star));

		ResTarget *rt_Astar = makeNode(ResTarget);
		rt_Astar->name = NIL;
		rt_Astar->indirection = NIL;
		rt_Astar->val = (Node *) cr_Astar;

		SelectStmt *sortedUnionStmt = makeNode(SelectStmt);
		sortedUnionStmt->fromClause = list_make1(subrange);
		sortedUnionStmt->sortClause = getSortListForSet(left_union->targetList);
		sortedUnionStmt->timeAdjustmentOperation = NIL;
		sortedUnionStmt->targetList = list_make1(rt_Astar); 

		RangeSubselect *subrangeSorted = makeNode(RangeSubselect);
		subrangeSorted->subquery = (Node *) sortedUnionStmt;
		subrangeSorted->alias = makeAlias("Sorted_UNION", NIL );

		// ----------------------------- ADJUSTED UNION ---------------------------

		ColumnRef *cr_Astar1 = makeNode(ColumnRef); // A_Star nodes (*) can only be at the end, as the grammar enforces this. -> SELECT TS, TE, *		
		cr_Astar1->fields = list_make1(makeNode(A_Star));

		ResTarget *rt_Astar1 = makeNode(ResTarget);
		rt_Astar1->name = NIL;
		rt_Astar1->indirection = NIL;
		rt_Astar1->val = (Node *) cr_Astar1;
		rt_Astar1->location = -1;

		// --------------------- Creation of the Union Node ---------------------
		SelectStmt *subselectForUnion = makeNode(SelectStmt);
		subselectForUnion->targetList = list_make1(rt_Astar1); 
		subselectForUnion->fromClause = list_make1(subrangeSorted);
        timeAdjustment->timeAdjustmentType = 5; // Adjustment Process for Set Operations (LAWA) 
		subselectForUnion->timeAdjustmentOperation = copyObject(timeAdjustment);
		timeAdjustment = NULL;

		// Add the Select stmt in a RangeSubselect
		RangeSubselect *subrangeSub = makeNode(RangeSubselect);
		subrangeSub->subquery = (Node *) subselectForUnion;
		subrangeSub->alias = makeAlias("ADJUSTED_UNION", NIL );

		// Lineage Fact
		ColumnRef *lin1_column_ref = makeNode(ColumnRef);
		lin1_column_ref->fields = list_make1(makeString("lin1"));

		ColumnRef *lin2_column_ref = makeNode(ColumnRef);
		lin2_column_ref->fields = list_make1(makeString("lin2"));

		A_Const *emptyString = makeNode(A_Const);
		emptyString->location = -1;
		emptyString->val = *makeString("");

		FuncCall   *lineageFunction = makeNode(FuncCall);
		lineageFunction->agg_order = NIL;
		lineageFunction->agg_filter = NULL;
		lineageFunction->agg_within_group = false;
		lineageFunction->agg_star = false;
		lineageFunction->agg_distinct = false;
		lineageFunction->func_variadic = false;
		lineageFunction->over = NULL;


		// Output lineage based on the type of the set operation
		if (stmt->op == SETOP_UNION) {
			lineageFunction->funcname = list_make1(makeString("concat_lineage_or"));	
		} else if (stmt->op == SETOP_INTERSECT) {
			lineageFunction->funcname = list_make1(makeString("concat_lineage_and"));	
			stmt->whereClause = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", (Node *) lin1_column_ref, (Node *) emptyString, -1);
			stmt->whereClause = (Node *) makeA_Expr(AEXPR_AND, NIL, stmt->whereClause, (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", (Node *) lin2_column_ref, (Node *) emptyString, -1), -1);
		} else {
			lineageFunction->funcname = list_make1(makeString("concat_lineage_andnot"));	
			stmt->whereClause = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", (Node *) lin1_column_ref, (Node *) emptyString, -1);
		}


		lineageFunction->args = list_make2(lin1_column_ref, lin2_column_ref);

		ResTarget *lineageTarget = makeNode(ResTarget);
		lineageTarget->val = (Node *) lineageFunction;
		lineageTarget->name = "lineage";
		lineageTarget->indirection = NULL;

		// Finalise the modification of the Stmt received as input in this function
		list_free(stmt->fromClause);
		stmt->fromClause = list_make1(subrangeSub);
		stmt->targetList = rightFact;
		stmt->targetList = lappend(stmt->targetList, get_res_target("ts", "ts" ));
		stmt->targetList = lappend(stmt->targetList, get_res_target("te", "te" ));


		stmt->targetList = lappend(stmt->targetList, lineageTarget);
		stmt->timeAdjustmentOperation = NIL;
		stmt->larg = NIL;
		stmt->rarg = NIL;
		stmt->op = SETOP_NONE;
		stmt->lineage_type = LINEAGE_NONE;
		stmt->dimension_type = DIMENSION_NONE;

	} else if (timeAdjustment->timeAdjustmentType == 5) {
		DB printf(" ------------------------------------- timeAdjustmentType = 5 --------------------------------------- \n\n");
	} else if (timeAdjustment->timeAdjustmentType == 6) {
		DB printf(" ------------------------------------- timeAdjustmentType = 5 --------------------------------------- \n\n");
	}

	else {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("undefined adjustment type %d !", timeAdjustment->timeAdjustmentType)));
	}

	fflush(stdout);

	DB printf("TRANSFORMING FROM CLAUSE - TIME/ADJ\n");

	// process the FROM clause
	transformFromClause(pstate, stmt->fromClause);

	fflush(stdout);
	DB printf("FROM CLAUSE DONE - TIME/ADJ\n");

	// transform targetlist
	qry->targetList = transformTargetList(pstate, stmt->targetList, EXPR_KIND_UPDATE_SOURCE);

	DB printf("TARGET LIST DONE - TIME/ADJ\n");

	// mark column origins
	markTargetListOrigins(pstate, qry->targetList);

	/* transform WHERE */
	qual = transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = transformWhereClause(pstate, stmt->havingClause, EXPR_KIND_HAVING, "HAVING");

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = transformSortClause(pstate, stmt->sortClause, &qry->targetList, EXPR_KIND_ORDER_BY, true /* fix unknowns */,
			false /* allow SQL92 rules */);

	qry->groupClause = transformGroupClause(pstate, stmt->groupClause, &qry->targetList, qry->sortClause, EXPR_KIND_GROUP_BY,
			false /* allow SQL92 rules */);

	if (stmt->distinctClause == NIL ) {
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	} else if (linitial(stmt->distinctClause) == NULL ) {
		// We had SELECT DISTINCT
		qry->distinctClause = transformDistinctClause(pstate, &qry->targetList, qry->sortClause, false );
		qry->hasDistinctOn = false;
	} else {
		// We had SELECT DISTINCT ON
		qry->distinctClause = transformDistinctOnClause(pstate, stmt->distinctClause, &qry->targetList, qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset, EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount, EXPR_KIND_LIMIT, "LIMIT");

	// transform window clauses after we have seen all window functions
	qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasAggs = pstate->p_hasAggs;
	if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	foreach(l, stmt->lockingClause)
	{
		transformLockingClause(pstate, qry, (LockingClause *) lfirst(l), false );
	}

	// TPDB - KATERINA
	// pass all attributes to the unification expression, "reuse" using list
	if (timeAdjustment != NULL) {
		transformTimeAdjustmentClause(pstate, &qry->targetList, timeAdjustment);
		qry->timeAdjustmentOperation = (Node *) timeAdjustment;
	}

	assign_query_collations(pstate, qry);

	return qry;
}

/* TPDB - KATERINA */
/* get_res_target
	- returns a ResTarget created that corresponds to a column with the same value
	  tag_value for all tuples
*/
static ResTarget*
make_tag_column(char* res_name, long tag_value) {

	A_Const *a_const = makeNode(A_Const);
	ResTarget *res_target = makeNode(ResTarget);

	a_const->location = -1;
	a_const->val = *makeInteger(tag_value);

	res_target->name = res_name;
	res_target->indirection = NIL;
	res_target->val = (Node *) a_const;
	res_target->location = -1;

	return res_target;
}

/* TPDB - KATERINA */
/* get_res_target
	- returns a ResTarget created based on a column-name and a relation-name
*/
static ResTarget*
get_res_target(char* column_name, char* res_name) {

	ColumnRef *column_ref = makeNode(ColumnRef);
	column_ref->fields = list_make1(makeString(column_name));
	column_ref->location = -1;

	ResTarget *res_target = makeNode(ResTarget);
	res_target->name = res_name;
	res_target->indirection = NIL;
	res_target->val = (Node *) column_ref;
	res_target->location = -1;
	return res_target;
}

/* TPDB - KATERINA */
/* printTargetList
	- prints the targetlist resT_list, used for debugging
*/
static void printTargetList(char* listname, List *resT_list) {
	ListCell *lc;
	ResTarget *rt_tmp;
	ColumnRef *cr_tmp;

	DB printf("Printing %s:", listname);

	foreach(lc, resT_list)
	{
		rt_tmp = (ResTarget *) lfirst(lc);

		if (rt_tmp->name != NULL) {
			char *l_colname = (char *) rt_tmp->name;

			cr_tmp = makeNode(ColumnRef);
			cr_tmp = rt_tmp->val;

			printf("%s, ", l_colname);
		} 
	}
	printf("\n\n");

	return;
}
 

/* TPDB - KATERINA */
static List *
getSortListForSet(List *resT_list) {
	/* Get the arguments in USING, make a ColumnRef and a ResTarget for each */
	ListCell *lc;
	ColumnRef *cr_tmp;
	SortBy *sort_item;
	List *starList = NIL;
	ResTarget *rt_tmp;

	DB printf("Making the sortlist\n");

	foreach(lc, resT_list)
	{
		rt_tmp = (ResTarget *) lfirst(lc);

		if (rt_tmp->name != NULL) {
			char *l_colname = (char *) rt_tmp->name;

			printf("SORT_CELL: %s \n", l_colname);
			if (strcmp(l_colname, "tag") == 0 || strcmp(l_colname, "lin1") == 0 || strcmp(l_colname, "lin2") ==0 
				|| strcmp(l_colname, "p1") == 0 || strcmp(l_colname, "p2") == 0 || strcmp(l_colname, "te") == 0 )  {
				continue;
			}
		} else {
			char *l_colname1;
		}
	
		cr_tmp = rt_tmp->val;

		sort_item = makeNode(SortBy);
		sort_item->node = (Node *) cr_tmp;
		sort_item->location = -1;
		starList = lappend(starList, sort_item);
		

	}

	return starList;

}


/* TPDB - KATERINA */
static List *
getSortListForOuterJoins(List *resT_list) {
	/* Get the arguments in USING, make a ColumnRef and a ResTarget for each */
	ListCell *lc;
	ColumnRef *cr_tmp;
	SortBy *sort_item;
	List *starList = NIL;
	ResTarget *rt_tmp;


	foreach(lc, resT_list)
	{
		rt_tmp = (ResTarget *) lfirst(lc);

		if (rt_tmp->name != NULL) {
			char *l_colname = (char *) rt_tmp->name;

			if (strcmp(l_colname, "lin1") == 0 || strcmp(l_colname, "lin2") ==0)  {
				continue;
			} else {
			}
		}
	
		cr_tmp = rt_tmp->val;
	
		sort_item = makeNode(SortBy);
		sort_item->node = (Node *) cr_tmp;
		sort_item->location = -1;
		starList = lappend(starList, sort_item);
		

	}

	return starList;

}


/* TPDB - KATERINA */
static List *
getJoinOuterFacts(List *resT_list) {
	/* Get the arguments in USING, make a ColumnRef and a ResTarget for each */
	ListCell *lc;
	ColumnRef *cr_tmp;
	SortBy *sort_item;
	List *starList = NIL;
	ResTarget *rt_tmp;

	foreach(lc, resT_list)
	{
		rt_tmp = (ResTarget *) lfirst(lc);

		if (rt_tmp->name != NULL) {

			ColumnRef *colRef = makeNode(ColumnRef);
			ResTarget *resTar = makeNode(ResTarget);

			char *l_colname = (char *) rt_tmp->name;

			if (strcmp(l_colname, "tag") == 0 || strcmp(l_colname, "p1") == 0 || strcmp(l_colname, "p2") == 0)  {
				continue;
			}

			colRef->fields = list_make1(makeString(l_colname));
			colRef->location = -1;
			resTar->val = (Node *) colRef;
			resTar->name = l_colname; // add column name 
			starList = lappend(starList, resTar);
		}
	}

	return starList;

}



static List * getNonTPColumnNamesFinalTarget(List *userTargetList, char* aliasname) {


	ListCell *lc;
	ColumnRef *cr_tmp;
	SortBy *sort_item;
	List *starList = NIL;
	ResTarget *rt_tmp;

	foreach(lc, userTargetList)
	{
		rt_tmp = (ResTarget *) lfirst(lc);

		if (rt_tmp->name != NULL) {



			ColumnRef *colRef = makeNode(ColumnRef);
			ResTarget *resTar = makeNode(ResTarget);
	
			// name of the attribute we need to put in the output
			ColumnRef *col_tmp = (ColumnRef *) rt_tmp->val;
			char *new_colnameInit = (char *) strVal(lsecond(col_tmp->fields));

			if (strcmp(rt_tmp->name, "ts") != 0 && strcmp(rt_tmp->name, "te") != 0) {

				int i=0;
				while (new_colnameInit[i] != '\0' && new_colnameInit[i] != '_') {
						i++;
				}

				char* new_colname = calloc(200, sizeof(char));
				if (new_colnameInit[i] != '\0') {
					i++;
					int j=0;
					while (new_colnameInit[i] != '\0') {
							new_colname[j] = new_colnameInit[i];
							i++; j++;
					}
					new_colname[i] = '\0';
				} else {
					strcpy(new_colname, "");
					strcpy(new_colname, new_colnameInit);
				}


				resTar->name = new_colname;
	
				char* old_colName = calloc(200, sizeof(char));
				strcpy(old_colName, "");
				strcpy(old_colName, rt_tmp->name);
				colRef->fields = list_make2(makeString(aliasname),makeString(old_colName));
	
				resTar->indirection = NIL;
				resTar->val = (Node *) colRef;
				resTar->location = -1;
	
				printf("Final_Target --- %s -- %s -- %s \n", aliasname, new_colname, old_colName);
				starList = lappend(starList, resTar);
			}
		}
	}

	printf("PROCESS_DONE\n");


	return starList;

}


/* TPDB - KATERINA */
/*
 * getNonTPColumnNamesForAStar -
 *	  makes a target list where the target for the attribute A of relation u has name u_A
 *
 */
static List *
getNonTPColumnNamesForAStar(ParseState *pstate, char* aliasname, Node *fromTree, bool dontIncludeLin) {

	RangeTblEntry *l_rte;
	int l_rtindex;
	List *l_namespace, *l_colnames, *l_colvars, *starList = NIL;

	/*
	 * Recursively process the left subtree, then the right.  We must do
	 * it in this order for correct visibility of LATERAL references.
	 */

	fromTree = transformFromClauseItem(pstate, fromTree, &l_rte, &l_rtindex, &l_namespace);

	expandRTE(l_rte, l_rtindex, 0, -1, false, &l_colnames, &l_colvars);

	ListCell *lx;

	foreach(lx, l_colnames)
	{
		char *l_colname = strVal(lfirst(lx));

		if (strcmp(l_colname, "lineage") != 0 && strcmp(l_colname, "p") != 0 && strcmp(l_colname, "ts") != 0
				&& strcmp(l_colname, "te") != 0) {

			ColumnRef *colRef = makeNode(ColumnRef);
			ResTarget *resTar = makeNode(ResTarget);
			colRef->location = -1;

			// The names of the columns consist of: Name_Of_Table + _ + Name_Of_Column
			// GOAL: to remove ambiguity
			if (dontIncludeLin) {
				if (strcmp(l_colname, "lin1") != 0 && strcmp(l_colname, "lin2") != 0) {
					colRef->fields = list_make2(makeString(aliasname),makeString(l_colname));
					resTar->name = NIL;		
				} else continue;
			
			}
			else {
				colRef->fields = list_make2(makeString(aliasname),makeString(l_colname));
				char* new_colName = calloc(200, sizeof(char));
				strcpy(new_colName, "");
				strcpy(new_colName, aliasname);
				strcat(new_colName, "_");
				strcat(new_colName, l_colname);
				resTar->name = new_colName;
			}

			resTar->indirection = NIL;
			resTar->val = (Node *) colRef;
			resTar->location = -1;

			DB printf("CURRENT_CELL: %s -- %s ---- ", l_colname, resTar->name);
			DB printf("CURRENT_TABLE: %s\n", aliasname);
			starList = lappend(starList, resTar);
		}
	}

	return starList;

}

/* TPDB - KATERINA */
/*
 * getNonTPForSet -
 *	  returns a targetlist with all the non-temporal, non-probabilistic attributes of the relation
 *	  with alias aliasname and represented in the node fromTree
 *
 */
static List *
getNonTPForSet(ParseState *pstate, char* aliasname, Node *fromTree) {
	/* A newfangled join expression */
	RangeTblEntry *l_rte;
	int l_rtindex;
	List *l_namespace, *l_colnames, *l_colvars, *starList = NIL;

	/*
	 * Recursively process the left subtree, then the right.  We must do
	 * it in this order for correct visibility of LATERAL references.
	 */
	fromTree = transformFromClauseItem(pstate, fromTree, &l_rte, &l_rtindex, &l_namespace);

	expandRTE(l_rte, l_rtindex, 0, -1, false, &l_colnames, &l_colvars);

	// List *rlist = NIL;
	ListCell *lx;

	foreach(lx, l_colnames)
	{
		char *l_colname = strVal(lfirst(lx));

		if (strcmp(l_colname, "lineage") != 0 && strcmp(l_colname, "p") != 0 && strcmp(l_colname, "ts") != 0
				&& strcmp(l_colname, "te") != 0) {

			ColumnRef *colRef = makeNode(ColumnRef);
			ResTarget *resTar = makeNode(ResTarget);
			colRef->fields = list_make1(makeString(l_colname));
			colRef->location = -1;
			resTar->name = NIL;

			resTar->indirection = NIL;
			resTar->val = (Node *) colRef;
			resTar->location = -1;

			DB printf("SET_CURRENT_CELL: %s -- %s\n", aliasname, l_colname);
			DB printf("SET_CURRENT_TABLE: %s\n", aliasname);
			starList = lappend(starList, resTar);
		}
	}

	//printf("END of THE first LIST\n");

	return starList;

}


/*
 * transformSetOperationStmt -
 *	  transforms a set-operations tree
 *
 * A set-operation tree is just a SELECT, but with UNION/INTERSECT/EXCEPT
 * structure to it.  We must transform each leaf SELECT and build up a top-
 * level Query that contains the leaf SELECTs as subqueries in its rangetable.
 * The tree of set operations is converted into the setOperations field of
 * the top-level Query.
 */
static Query *
transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt) {
	Query *qry = makeNode(Query);
	SelectStmt *leftmostSelect;
	int leftmostRTI;
	Query *leftmostQuery;
	SetOperationStmt *sostmt;
	List *sortClause;
	Node *limitOffset;
	Node *limitCount;
	List *lockingClause;
	WithClause *withClause;
	Node *node;
	ListCell *left_tlist, *lct, *lcm, *lcc, *l;
	List *targetvars, *targetnames, *sv_namespace;
	int sv_rtable_length;
	RangeTblEntry *jrte;
	int tllen;

	qry->commandType = CMD_SELECT;

	/*
	 * Find leftmost leaf SelectStmt.  We currently only need to do this in
	 * order to deliver a suitable error message if there's an INTO clause
	 * there, implying the set-op tree is in a context that doesn't allow
	 * INTO.  (transformSetOperationTree would throw error anyway, but it
	 * seems worth the trouble to throw a different error for non-leftmost
	 * INTO, so we produce that error in transformSetOperationTree.)
	 */
	leftmostSelect = stmt->larg;
	while (leftmostSelect && leftmostSelect->op != SETOP_NONE)
		leftmostSelect = leftmostSelect->larg;
	Assert(leftmostSelect && IsA(leftmostSelect, SelectStmt) &&
			leftmostSelect->larg == NULL);
	if (leftmostSelect->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("SELECT ... INTO is not allowed here"), parser_errposition(pstate, exprLocation((Node *) leftmostSelect->intoClause))));

	/*
	 * We need to extract ORDER BY and other top-level clauses here and not
	 * let transformSetOperationTree() see them --- else it'll just recurse
	 * right back here!
	 */
	sortClause = stmt->sortClause;
	limitOffset = stmt->limitOffset;
	limitCount = stmt->limitCount;
	lockingClause = stmt->lockingClause;
	withClause = stmt->withClause;

	stmt->sortClause = NIL;
	stmt->limitOffset = NULL;
	stmt->limitCount = NULL;
	stmt->lockingClause = NIL;
	stmt->withClause = NULL;

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (lockingClause)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT", LCS_asString(((LockingClause *) linitial(lockingClause))->strength))));

	/* Process the WITH clause independently of all else */
	if (withClause) {
		qry->hasRecursive = withClause->recursive;
		qry->cteList = transformWithClause(pstate, withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * Recursively transform the components of the tree.
	 */
	sostmt = (SetOperationStmt *) transformSetOperationTree(pstate, stmt, true, NULL );
	Assert(sostmt && IsA(sostmt, SetOperationStmt));
	qry->setOperations = (Node *) sostmt;

	/*
	 * Re-find leftmost SELECT (now it's a sub-query in rangetable)
	 */
	node = sostmt->larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable) ->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist for outer query using column names of
	 * leftmost select and common datatypes/collations of topmost set
	 * operation.  Also make lists of the dummy vars and their names for use
	 * in parsing ORDER BY.
	 *
	 * Note: we use leftmostRTI as the varno of the dummy variables. It
	 * shouldn't matter too much which RT index they have, as long as they
	 * have one that corresponds to a real RT entry; else funny things may
	 * happen when the tree is mashed by rule rewriting.
	 */
	qry->targetList = NIL;
	targetvars = NIL;
	targetnames = NIL;
	left_tlist = list_head(leftmostQuery->targetList);

	forthree(lct, sostmt->colTypes,
			lcm, sostmt->colTypmods,
			lcc, sostmt->colCollations)
	{
		Oid colType = lfirst_oid(lct);
		int32 colTypmod = lfirst_int(lcm);
		Oid colCollation = lfirst_oid(lcc);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char *colName;
		TargetEntry *tle;
		Var *var;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		var = makeVar(leftmostRTI, lefttle->resno, colType, colTypmod, colCollation, 0);
		var->location = exprLocation((Node *) lefttle->expr);
		tle = makeTargetEntry((Expr *) var, (AttrNumber) pstate->p_next_resno++, colName, false );
		qry->targetList = lappend(qry->targetList, tle);
		targetvars = lappend(targetvars, var);
		targetnames = lappend(targetnames, makeString(colName));
		left_tlist = lnext(left_tlist);
	}

	/*
	 * As a first step towards supporting sort clauses that are expressions
	 * using the output columns, generate a namespace entry that makes the
	 * output columns visible.  A Join RTE node is handy for this, since we
	 * can easily control the Vars generated upon matches.
	 *
	 * Note: we don't yet do anything useful with such cases, but at least
	 * "ORDER BY upper(foo)" will draw the right error message rather than
	 * "foo not found".
	 */
	sv_rtable_length = list_length(pstate->p_rtable);

	jrte = addRangeTableEntryForJoin(pstate, targetnames, JOIN_INNER, targetvars, NULL, false );

	sv_namespace = pstate->p_namespace;
	pstate->p_namespace = NIL;

	/* add jrte to column namespace only */
	addRTEtoQuery(pstate, jrte, false, false, true );

	/*
	 * For now, we don't support resjunk sort clauses on the output of a
	 * setOperation tree --- you can only use the SQL92-spec options of
	 * selecting an output column by name or number.  Enforce by checking that
	 * transformSortClause doesn't add any items to tlist.
	 */
	tllen = list_length(qry->targetList);

	qry->sortClause = transformSortClause(pstate, sortClause, &qry->targetList, EXPR_KIND_ORDER_BY, false /* no unknowns expected */,
			false /* allow SQL92 rules */);

	/* restore namespace, remove jrte from rtable */
	pstate->p_namespace = sv_namespace;
	pstate->p_rtable = list_truncate(pstate->p_rtable, sv_rtable_length);

	if (tllen != list_length(qry->targetList))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid UNION/INTERSECT/EXCEPT ORDER BY clause"), errdetail("Only result column names can be used, not expressions or functions."), errhint("Add the expression/function to every SELECT, or move the UNION into a FROM clause."), parser_errposition(pstate, exprLocation(list_nth(qry->targetList, tllen)))));

	qry->limitOffset = transformLimitClause(pstate, limitOffset, EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, limitCount, EXPR_KIND_LIMIT, "LIMIT");

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL );

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasAggs = pstate->p_hasAggs;
	if (pstate->p_hasAggs || qry->groupClause || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	foreach(l, lockingClause)
	{
		transformLockingClause(pstate, qry, (LockingClause *) lfirst(l), false );
	}

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformSetOperationTree
 *		Recursively transform leaves and internal nodes of a set-op tree
 *
 * In addition to returning the transformed node, if targetlist isn't NULL
 * then we return a list of its non-resjunk TargetEntry nodes.  For a leaf
 * set-op node these are the actual targetlist entries; otherwise they are
 * dummy entries created to carry the type, typmod, collation, and location
 * (for error messages) of each output column of the set-op node.  This info
 * is needed only during the internal recursion of this function, so outside
 * callers pass NULL for targetlist.  Note: the reason for passing the
 * actual targetlist entries of a leaf node is so that upper levels can
 * replace UNKNOWN Consts with properly-coerced constants.
 */
static Node *
transformSetOperationTree(ParseState *pstate, SelectStmt *stmt, bool isTopLevel, List **targetlist) {
	bool isLeaf;

	Assert(stmt && IsA(stmt, SelectStmt));

	/* Guard against stack overflow due to overly complex set-expressions */
	check_stack_depth();

	/*
	 * Validity-check both leaf and internal SELECTs for disallowed ops.
	 */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT"), parser_errposition(pstate, exprLocation((Node *) stmt->intoClause))));

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (stmt->lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				/*------
				 translator: %s is a SQL row locking clause such as FOR UPDATE */
				errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT", LCS_asString(((LockingClause *) linitial(stmt->lockingClause))->strength))));

	/*
	 * If an internal node of a set-op tree has ORDER BY, LIMIT, FOR UPDATE,
	 * or WITH clauses attached, we need to treat it like a leaf node to
	 * generate an independent sub-Query tree.  Otherwise, it can be
	 * represented by a SetOperationStmt node underneath the parent Query.
	 */
	if (stmt->op == SETOP_NONE) {
		Assert(stmt->larg == NULL && stmt->rarg == NULL);
		isLeaf = true;
	} else {
		Assert(stmt->larg != NULL && stmt->rarg != NULL);
		if (stmt->sortClause || stmt->limitOffset || stmt->limitCount || stmt->lockingClause || stmt->withClause)
			isLeaf = true;
		else
			isLeaf = false;
	}

	if (isLeaf) {
		/* Process leaf SELECT */
		Query *selectQuery;
		char selectName[32];
		RangeTblEntry *rte PG_USED_FOR_ASSERTS_ONLY;
		RangeTblRef *rtr;
		ListCell *tl;

		/*
		 * Transform SelectStmt into a Query.
		 *
		 * Note: previously transformed sub-queries don't affect the parsing
		 * of this sub-query, because they are not in the toplevel pstate's
		 * namespace list.
		 */
		selectQuery = parse_sub_analyze((Node *) stmt, pstate, NULL, false );

		/*
		 * Check for bogus references to Vars on the current query level (but
		 * upper-level references are okay). Normally this can't happen
		 * because the namespace will be empty, but it could happen if we are
		 * inside a rule.
		 */
		if (pstate->p_namespace) {
			if (contain_vars_of_level((Node *) selectQuery, 1))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query level"), parser_errposition(pstate, locate_var_of_level((Node *) selectQuery, 1))));
		}

		/*
		 * Extract a list of the non-junk TLEs for upper-level processing.
		 */
		if (targetlist) {
			*targetlist = NIL;
			foreach(tl, selectQuery->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

				if (!tle->resjunk)
					*targetlist = lappend(*targetlist, tle);
			}
		}

		/*
		 * Make the leaf query be a subquery in the top-level rangetable.
		 */
		snprintf(selectName, sizeof(selectName), "*SELECT* %d", list_length(pstate->p_rtable) + 1);
		rte = addRangeTableEntryForSubquery(pstate, selectQuery, makeAlias(selectName, NIL ), false, false );

		/*
		 * Return a RangeTblRef to replace the SelectStmt in the set-op tree.
		 */
		rtr = makeNode(RangeTblRef);
		/* assume new rte is at end */
		rtr->rtindex = list_length(pstate->p_rtable);
		Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
		return (Node *) rtr;
	} else {
		/* Process an internal node (set operation node) */
		SetOperationStmt *op = makeNode(SetOperationStmt);
		List *ltargetlist;
		List *rtargetlist;
		ListCell *ltl;
		ListCell *rtl;
		const char *context;

		context = (stmt->op == SETOP_UNION ? "UNION" : (stmt->op == SETOP_INTERSECT ? "INTERSECT" : "EXCEPT"));

		op->op = stmt->op;
		op->all = stmt->all;

		/*
		 * Recursively transform the left child node.
		 */
		op->larg = transformSetOperationTree(pstate, stmt->larg, false, &ltargetlist);

		/*
		 * If we are processing a recursive union query, now is the time to
		 * examine the non-recursive term's output columns and mark the
		 * containing CTE as having those result columns.  We should do this
		 * only at the topmost setop of the CTE, of course.
		 */
		if (isTopLevel && pstate->p_parent_cte && pstate->p_parent_cte->cterecursive)
			determineRecursiveColTypes(pstate, op->larg, ltargetlist);

		/*
		 * Recursively transform the right child node.
		 */
		op->rarg = transformSetOperationTree(pstate, stmt->rarg, false, &rtargetlist);

		/*
		 * Verify that the two children have the same number of non-junk
		 * columns, and determine the types of the merged output columns.
		 */
		if (list_length(ltargetlist) != list_length(rtargetlist))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR), errmsg("each %s query must have the same number of columns", context), parser_errposition(pstate, exprLocation((Node *) rtargetlist))));

		if (targetlist)
			*targetlist = NIL;
		op->colTypes = NIL;
		op->colTypmods = NIL;
		op->colCollations = NIL;
		op->groupClauses = NIL;
		forboth(ltl, ltargetlist, rtl, rtargetlist)
		{
			TargetEntry *ltle = (TargetEntry *) lfirst(ltl);
			TargetEntry *rtle = (TargetEntry *) lfirst(rtl);
			Node *lcolnode = (Node *) ltle->expr;
			Node *rcolnode = (Node *) rtle->expr;
			Oid lcoltype = exprType(lcolnode);
			Oid rcoltype = exprType(rcolnode);
			int32 lcoltypmod = exprTypmod(lcolnode);
			int32 rcoltypmod = exprTypmod(rcolnode);
			Node *bestexpr;
			int bestlocation;
			Oid rescoltype;
			int32 rescoltypmod;
			Oid rescolcoll;

			/* select common type, same as CASE et al */
			rescoltype = select_common_type(pstate, list_make2(lcolnode, rcolnode), context, &bestexpr);
			bestlocation = exprLocation(bestexpr);
			/* if same type and same typmod, use typmod; else default */
			if (lcoltype == rcoltype && lcoltypmod == rcoltypmod)
				rescoltypmod = lcoltypmod;
			else
				rescoltypmod = -1;

			/*
			 * Verify the coercions are actually possible.  If not, we'd fail
			 * later anyway, but we want to fail now while we have sufficient
			 * context to produce an error cursor position.
			 *
			 * For all non-UNKNOWN-type cases, we verify coercibility but we
			 * don't modify the child's expression, for fear of changing the
			 * child query's semantics.
			 *
			 * If a child expression is an UNKNOWN-type Const or Param, we
			 * want to replace it with the coerced expression.  This can only
			 * happen when the child is a leaf set-op node.  It's safe to
			 * replace the expression because if the child query's semantics
			 * depended on the type of this output column, it'd have already
			 * coerced the UNKNOWN to something else.  We want to do this
			 * because (a) we want to verify that a Const is valid for the
			 * target type, or resolve the actual type of an UNKNOWN Param,
			 * and (b) we want to avoid unnecessary discrepancies between the
			 * output type of the child query and the resolved target type.
			 * Such a discrepancy would disable optimization in the planner.
			 *
			 * If it's some other UNKNOWN-type node, eg a Var, we do nothing
			 * (knowing that coerce_to_common_type would fail).  The planner
			 * is sometimes able to fold an UNKNOWN Var to a constant before
			 * it has to coerce the type, so failing now would just break
			 * cases that might work.
			 */
			if (lcoltype != UNKNOWNOID)
				lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
			else if (IsA(lcolnode, Const) || IsA(lcolnode, Param)) {
				lcolnode = coerce_to_common_type(pstate, lcolnode, rescoltype, context);
				ltle->expr = (Expr *) lcolnode;
			}

			if (rcoltype != UNKNOWNOID)
				rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
			else if (IsA(rcolnode, Const) || IsA(rcolnode, Param)) {
				rcolnode = coerce_to_common_type(pstate, rcolnode, rescoltype, context);
				rtle->expr = (Expr *) rcolnode;
			}

			/*
			 * Select common collation.  A common collation is required for
			 * all set operators except UNION ALL; see SQL:2008 7.13 <query
			 * expression> Syntax Rule 15c.  (If we fail to identify a common
			 * collation for a UNION ALL column, the curCollations element
			 * will be set to InvalidOid, which may result in a runtime error
			 * if something at a higher query level wants to use the column's
			 * collation.)
			 */
			rescolcoll = select_common_collation(pstate, list_make2(lcolnode, rcolnode), (op->op == SETOP_UNION && op->all));

			/* emit results */
			op->colTypes = lappend_oid(op->colTypes, rescoltype);
			op->colTypmods = lappend_int(op->colTypmods, rescoltypmod);
			op->colCollations = lappend_oid(op->colCollations, rescolcoll);

			/*
			 * For all cases except UNION ALL, identify the grouping operators
			 * (and, if available, sorting operators) that will be used to
			 * eliminate duplicates.
			 */
			if (op->op != SETOP_UNION || !op->all) {
				SortGroupClause *grpcl = makeNode(SortGroupClause);
				Oid sortop;
				Oid eqop;
				bool hashable;
				ParseCallbackState pcbstate;

				setup_parser_errposition_callback(&pcbstate, pstate, bestlocation);

				/* determine the eqop and optional sortop */
				get_sort_group_operators(rescoltype, false, true, false, &sortop, &eqop, NULL, &hashable);

				cancel_parser_errposition_callback(&pcbstate);

				/* we don't have a tlist yet, so can't assign sortgrouprefs */
				grpcl->tleSortGroupRef = 0;
				grpcl->eqop = eqop;
				grpcl->sortop = sortop;
				grpcl->nulls_first = false; /* OK with or without sortop */
				grpcl->hashable = hashable;

				op->groupClauses = lappend(op->groupClauses, grpcl);
			}

			/*
			 * Construct a dummy tlist entry to return.  We use a SetToDefault
			 * node for the expression, since it carries exactly the fields
			 * needed, but any other expression node type would do as well.
			 */
			if (targetlist) {
				SetToDefault *rescolnode = makeNode(SetToDefault);
				TargetEntry *restle;

				rescolnode->typeId = rescoltype;
				rescolnode->typeMod = rescoltypmod;
				rescolnode->collation = rescolcoll;
				rescolnode->location = bestlocation;
				restle = makeTargetEntry((Expr *) rescolnode, 0, /* no need to set resno */
				NULL, false );
				*targetlist = lappend(*targetlist, restle);
			}
		}

		return (Node *) op;
	}
}

/*
 * Process the outputs of the non-recursive term of a recursive union
 * to set up the parent CTE's columns
 */
static void determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist) {
	Node *node;
	int leftmostRTI;
	Query *leftmostQuery;
	List *targetList;
	ListCell *left_tlist;
	ListCell *nrtl;
	int next_resno;

	/*
	 * Find leftmost leaf SELECT
	 */
	node = larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable) ->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist using column names of leftmost select and
	 * dummy result expressions of the non-recursive term.
	 */
	targetList = NIL;
	left_tlist = list_head(leftmostQuery->targetList);
	next_resno = 1;

	foreach(nrtl, nrtargetlist)
	{
		TargetEntry *nrtle = (TargetEntry *) lfirst(nrtl);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char *colName;
		TargetEntry *tle;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		tle = makeTargetEntry(nrtle->expr, next_resno++, colName, false );
		targetList = lappend(targetList, tle);
		left_tlist = lnext(left_tlist);
	}

	/* Now build CTE's output column info using dummy targetlist */
	analyzeCTETargetList(pstate, pstate->p_parent_cte, targetList);
}

/*
 * transformUpdateStmt -
 *	  transforms an update statement
 */
static Query *
transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt) {
	Query *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	RangeTblEntry *target_rte;
	Node *qual;
	ListCell *origTargetList;
	ListCell *tl;

	qry->commandType = CMD_UPDATE;
	pstate->p_is_update = true;

	/* process the WITH clause independently of all else */
	if (stmt->withClause) {
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	qry->resultRelation = setTargetTable(pstate, stmt->relation, interpretInhOption(stmt->relation->inhOpt), true, ACL_UPDATE);

	/* grab the namespace item made by setTargetTable */
	nsitem = (ParseNamespaceItem *) llast(pstate->p_namespace);

	/* subqueries in FROM cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * the FROM clause is non-standard SQL syntax. We used to be able to do
	 * this with REPLACE in POSTQUEL so we keep the feature.
	 */
	transformFromClause(pstate, stmt->fromClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	qry->targetList = transformTargetList(pstate, stmt->targetList, EXPR_KIND_UPDATE_SOURCE);

	qual = transformWhereClause(pstate, stmt->whereClause, EXPR_KIND_WHERE, "WHERE");

	qry->returningList = transformReturningList(pstate, stmt->returningList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	/*
	 * Now we are done with SELECT-like processing, and can get on with
	 * transforming the target list to match the UPDATE target columns.
	 */

	/* Prepare to assign non-conflicting resnos to resjunk attributes */
	if (pstate->p_next_resno <= pstate->p_target_relation->rd_rel->relnatts)
		pstate->p_next_resno = pstate->p_target_relation->rd_rel->relnatts + 1;

	/* Prepare non-junk columns for assignment to target table */
	target_rte = pstate->p_target_rangetblentry;
	origTargetList = list_head(stmt->targetList);

	foreach(tl, qry->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		ResTarget *origTarget;
		int attrno;

		if (tle->resjunk) {
			/*
			 * Resjunk nodes need no additional processing, but be sure they
			 * have resnos that do not match any target columns; else rewriter
			 * or planner might get confused.  They don't need a resname
			 * either.
			 */
			tle->resno = (AttrNumber) pstate->p_next_resno++;
			tle->resname = NULL;
			continue;
		}
		if (origTargetList == NULL )
			elog(ERROR, "UPDATE target count mismatch --- internal error");
		origTarget = (ResTarget *) lfirst(origTargetList);
		Assert(IsA(origTarget, ResTarget));

		attrno = attnameAttNum(pstate->p_target_relation, origTarget->name, true );
		if (attrno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" of relation \"%s\" does not exist", origTarget->name, RelationGetRelationName(pstate->p_target_relation)), parser_errposition(pstate, origTarget->location)));

		updateTargetListEntry(pstate, tle, origTarget->name, attrno, origTarget->indirection, origTarget->location);

		/* Mark the target column as requiring update permissions */
		target_rte->modifiedCols = bms_add_member(target_rte->modifiedCols, attrno - FirstLowInvalidHeapAttributeNumber);

		origTargetList = lnext(origTargetList);
	}
	if (origTargetList != NULL )
		elog(ERROR, "UPDATE target count mismatch --- internal error");

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformReturningList -
 *	handle a RETURNING clause in INSERT/UPDATE/DELETE
 */
static List *
transformReturningList(ParseState *pstate, List *returningList) {
	List *rlist;
	int save_next_resno;

	if (returningList == NIL )
		return NIL ; /* nothing to do */

	/*
	 * We need to assign resnos starting at one in the RETURNING list. Save
	 * and restore the main tlist's value of p_next_resno, just in case
	 * someone looks at it later (probably won't happen).
	 */
	save_next_resno = pstate->p_next_resno;
	pstate->p_next_resno = 1;

	/* transform RETURNING identically to a SELECT targetlist */
	rlist = transformTargetList(pstate, returningList, EXPR_KIND_RETURNING);

	/*
	 * Complain if the nonempty tlist expanded to nothing (which is possible
	 * if it contains only a star-expansion of a zero-column table).  If we
	 * allow this, the parsed Query will look like it didn't have RETURNING,
	 * with results that would probably surprise the user.
	 */
	if (rlist == NIL )
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), errmsg("RETURNING must have at least one column"), parser_errposition(pstate, exprLocation(linitial(returningList)))));

	/* mark column origins */
	markTargetListOrigins(pstate, rlist);

	/* restore state */
	pstate->p_next_resno = save_next_resno;

	return rlist;
}

/*
 * transformDeclareCursorStmt -
 *	transform a DECLARE CURSOR Statement
 *
 * DECLARE CURSOR is a hybrid case: it's an optimizable statement (in fact not
 * significantly different from a SELECT) as far as parsing/rewriting/planning
 * are concerned, but it's not passed to the executor and so in that sense is
 * a utility statement.  We transform it into a Query exactly as if it were
 * a SELECT, then stick the original DeclareCursorStmt into the utilityStmt
 * field to carry the cursor name and options.
 */
static Query *
transformDeclareCursorStmt(ParseState *pstate, DeclareCursorStmt *stmt) {
	Query *result;

	/*
	 * Don't allow both SCROLL and NO SCROLL to be specified
	 */
	if ((stmt->options & CURSOR_OPT_SCROLL) && (stmt->options & CURSOR_OPT_NO_SCROLL))
		ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_DEFINITION), errmsg("cannot specify both SCROLL and NO SCROLL")));

	result = transformStmt(pstate, stmt->query);

	/* Grammar should not have allowed anything but SELECT */
	if (!IsA(result, Query) || result->commandType != CMD_SELECT || result->utilityStmt != NULL )
		elog(ERROR, "unexpected non-SELECT command in DECLARE CURSOR");

	/*
	 * We also disallow data-modifying WITH in a cursor.  (This could be
	 * allowed, but the semantics of when the updates occur might be
	 * surprising.)
	 */
	if (result->hasModifyingCTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")));

	/* FOR UPDATE and WITH HOLD are not compatible */
	if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_HOLD))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				/*------
				 translator: %s is a SQL row locking clause such as FOR UPDATE */
				errmsg("DECLARE CURSOR WITH HOLD ... %s is not supported", LCS_asString(((RowMarkClause *) linitial(result->rowMarks))->strength)), errdetail("Holdable cursors must be READ ONLY.")));

	/* FOR UPDATE and SCROLL are not compatible */
	if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_SCROLL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				/*------
				 translator: %s is a SQL row locking clause such as FOR UPDATE */
				errmsg("DECLARE SCROLL CURSOR ... %s is not supported", LCS_asString(((RowMarkClause *) linitial(result->rowMarks))->strength)), errdetail("Scrollable cursors must be READ ONLY.")));

	/* FOR UPDATE and INSENSITIVE are not compatible */
	if (result->rowMarks != NIL && (stmt->options & CURSOR_OPT_INSENSITIVE))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				/*------
				 translator: %s is a SQL row locking clause such as FOR UPDATE */
				errmsg("DECLARE INSENSITIVE CURSOR ... %s is not supported", LCS_asString(((RowMarkClause *) linitial(result->rowMarks))->strength)), errdetail("Insensitive cursors must be READ ONLY.")));

	/* We won't need the raw querytree any more */
	stmt->query = NULL;

	result->utilityStmt = (Node *) stmt;

	return result;
}

/*
 * transformExplainStmt -
 *	transform an EXPLAIN Statement
 *
 * EXPLAIN is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query *
transformExplainStmt(ParseState *pstate, ExplainStmt *stmt) {
	Query *result;

	/* transform contained query, allowing SELECT INTO */
	stmt->query = (Node *) transformTopLevelStmt(pstate, stmt->query);

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

/*
 * transformCreateTableAsStmt -
 *	transform a CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW
 *	Statement
 *
 * As with EXPLAIN, transform the contained statement now.
 */
static Query *
transformCreateTableAsStmt(ParseState *pstate, CreateTableAsStmt *stmt) {
	Query *result;
	Query *query;

	/* transform contained query */
	query = transformStmt(pstate, stmt->query);
	stmt->query = (Node *) query;

	/* additional work needed for CREATE MATERIALIZED VIEW */
	if (stmt->relkind == OBJECT_MATVIEW) {
		/*
		 * Prohibit a data-modifying CTE in the query used to create a
		 * materialized view. It's not sufficiently clear what the user would
		 * want to happen if the MV is refreshed or incrementally maintained.
		 */
		if (query->hasModifyingCTE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialized views must not use data-modifying statements in WITH")));

		/*
		 * Check whether any temporary database objects are used in the
		 * creation query. It would be hard to refresh data or incrementally
		 * maintain it if a source disappeared.
		 */
		if (isQueryUsingTempRelation(query))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialized views must not use temporary tables or views")));

		/*
		 * A materialized view would either need to save parameters for use in
		 * maintaining/loading the data or prohibit them entirely.  The latter
		 * seems safer and more sane.
		 */
		if (query_contains_extern_params(query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialized views may not be defined using bound parameters")));

		/*
		 * For now, we disallow unlogged materialized views, because it seems
		 * like a bad idea for them to just go to empty after a crash. (If we
		 * could mark them as unpopulated, that would be better, but that
		 * requires catalog changes which crash recovery can't presently
		 * handle.)
		 */
		if (stmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialized views cannot be UNLOGGED")));

		/*
		 * At runtime, we'll need a copy of the parsed-but-not-rewritten Query
		 * for purposes of creating the view's ON SELECT rule.  We stash that
		 * in the IntoClause because that's where intorel_startup() can
		 * conveniently get it from.
		 */
		stmt->into->viewQuery = copyObject(query);
	}

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

char *
LCS_asString(LockClauseStrength strength) {
	switch (strength) {
	case LCS_FORKEYSHARE:
		return "FOR KEY SHARE";
	case LCS_FORSHARE:
		return "FOR SHARE";
	case LCS_FORNOKEYUPDATE:
		return "FOR NO KEY UPDATE";
	case LCS_FORUPDATE:
		return "FOR UPDATE";
	}
	return "FOR some"; /* shouldn't happen */
}

/*
 * Check for features that are not supported with FOR [KEY] UPDATE/SHARE.
 *
 * exported so planner can check again after rewriting, query pullup, etc
 */
void CheckSelectLocking(Query *qry, LockClauseStrength strength) {
	if (qry->setOperations)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT", LCS_asString(strength))));
	if (qry->distinctClause != NIL )
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with DISTINCT clause", LCS_asString(strength))));
	if (qry->groupClause != NIL )
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with GROUP BY clause", LCS_asString(strength))));
	if (qry->havingQual != NULL )
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with HAVING clause", LCS_asString(strength))));
	if (qry->hasAggs)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with aggregate functions", LCS_asString(strength))));
	if (qry->hasWindowFuncs)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with window functions", LCS_asString(strength))));
	if (expression_returns_set((Node *) qry->targetList))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		 translator: %s is a SQL row locking clause such as FOR UPDATE */
		errmsg("%s is not allowed with set-returning functions in the target list", LCS_asString(strength))));
}

/*
 * Transform a FOR [KEY] UPDATE/SHARE clause
 *
 * This basically involves replacing names by integer relids.
 *
 * NB: if you need to change this, see also markQueryForLocking()
 * in rewriteHandler.c, and isLockedRefname() in parse_relation.c.
 */
static void transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc, bool pushedDown) {
	List *lockedRels = lc->lockedRels;
	ListCell *l;
	ListCell *rt;
	Index i;
	LockingClause *allrels;

	CheckSelectLocking(qry, lc->strength);

	/* make a clause we can pass down to subqueries to select all rels */
	allrels = makeNode(LockingClause);
	allrels->lockedRels = NIL; /* indicates all rels */
	allrels->strength = lc->strength;
	allrels->noWait = lc->noWait;

	if (lockedRels == NIL ) {
		/* all regular tables used in query */
		i = 0;
		foreach(rt, qry->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

			++i;
			switch (rte->rtekind) {
			case RTE_RELATION:
				applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown);
				rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
				break;
			case RTE_SUBQUERY:
				applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown);

				/*
				 * FOR UPDATE/SHARE of subquery is propagated to all of
				 * subquery's rels, too.  We could do this later (based on
				 * the marking of the subquery RTE) but it is convenient
				 * to have local knowledge in each query level about which
				 * rels need to be opened with RowShareLock.
				 */
				transformLockingClause(pstate, rte->subquery, allrels, true );
				break;
			default:
				/* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
				break;
			}
		}
	} else {
		/* just the named tables */
		foreach(l, lockedRels)
		{
			RangeVar *thisrel = (RangeVar *) lfirst(l);

			/* For simplicity we insist on unqualified alias names here */
			if (thisrel->catalogname || thisrel->schemaname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						/*------
						 translator: %s is a SQL row locking clause such as FOR UPDATE */
						errmsg("%s must specify unqualified relation names", LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));

			i = 0;
			foreach(rt, qry->rtable)
			{
				RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

				++i;
				if (strcmp(rte->eref->aliasname, thisrel->relname) == 0) {
					switch (rte->rtekind) {
					case RTE_RELATION:
						applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown);
						rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
						break;
					case RTE_SUBQUERY:
						applyLockingClause(qry, i, lc->strength, lc->noWait, pushedDown);
						/* see comment above */
						transformLockingClause(pstate, rte->subquery, allrels, true );
						break;
					case RTE_JOIN:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								/*------
								 translator: %s is a SQL row locking clause such as FOR UPDATE */
								errmsg("%s cannot be applied to a join", LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));
						break;
					case RTE_FUNCTION:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								/*------
								 translator: %s is a SQL row locking clause such as FOR UPDATE */
								errmsg("%s cannot be applied to a function", LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));
						break;
					case RTE_VALUES:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								/*------
								 translator: %s is a SQL row locking clause such as FOR UPDATE */
								errmsg("%s cannot be applied to VALUES", LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));
						break;
					case RTE_CTE:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								/*------
								 translator: %s is a SQL row locking clause such as FOR UPDATE */
								errmsg("%s cannot be applied to a WITH query", LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));
						break;
					default:
						elog(ERROR, "unrecognized RTE type: %d", (int) rte->rtekind);
						break;
					}
					break; /* out of foreach loop */
				}
			}
			if (rt == NULL )
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_TABLE),
						/*------
						 translator: %s is a SQL row locking clause such as FOR UPDATE */
						errmsg("relation \"%s\" in %s clause not found in FROM clause", thisrel->relname, LCS_asString(lc->strength)), parser_errposition(pstate, thisrel->location)));
		}
	}
}

/*
 * Record locking info for a single rangetable item
 */
void applyLockingClause(Query *qry, Index rtindex, LockClauseStrength strength, bool noWait, bool pushedDown) {
	RowMarkClause *rc;

	/* If it's an explicit clause, make sure hasForUpdate gets set */
	if (!pushedDown)
		qry->hasForUpdate = true;

	/* Check for pre-existing entry for same rtindex */
	if ((rc = get_parse_rowmark(qry, rtindex)) != NULL ) {
		/*
		 * If the same RTE is specified for more than one locking strength,
		 * treat is as the strongest.  (Reasonable, since you can't take both
		 * a shared and exclusive lock at the same time; it'll end up being
		 * exclusive anyway.)
		 *
		 * We also consider that NOWAIT wins if it's specified both ways. This
		 * is a bit more debatable but raising an error doesn't seem helpful.
		 * (Consider for instance SELECT FOR UPDATE NOWAIT from a view that
		 * internally contains a plain FOR UPDATE spec.)
		 *
		 * And of course pushedDown becomes false if any clause is explicit.
		 */
		rc->strength = Max(rc->strength, strength);
		rc->noWait |= noWait;
		rc->pushedDown &= pushedDown;
		return;
	}

	/* Make a new RowMarkClause */
	rc = makeNode(RowMarkClause);
	rc->rti = rtindex;
	rc->strength = strength;
	rc->noWait = noWait;
	rc->pushedDown = pushedDown;
	qry->rowMarks = lappend(qry->rowMarks, rc);
}