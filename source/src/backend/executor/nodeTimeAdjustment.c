/*
 * nodeTimeAdjustment.c
 *
 * TPDB - KATERINA
 *      
 */

// #define DEBUG_UO 1
// #define DEBUG_BOTH 1
// #define DEBUG_N 1
// #define DEBUG_SET 1
// #define DEBUG_SET1 1

#ifdef DEBUG_SET1
 #define DS1
#else
 #define DS1 for(;0;)
#endif

#ifdef DEBUG_SET
 #define DS 
#else
 #define DS for(;0;)
#endif

#ifdef DEBUG_N
 #define D 
#else
 #define D for(;0;)
#endif

#ifdef DEBUG_UO
 #define D1
#else
 #define D1 for(;0;)
#endif

#ifdef DEBUG_LIST
 #define DL
#else
 #define DL for(;0;)
#endif

#ifdef DEBUG_BOTH
 #define DB
#else
 #define DB for(;0;)
#endif

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeTimeAdjustment.h"
#include "executor/nodeUnique.h"
#include "utils/memutils.h"
#include "access/htup_details.h"
#include "../include/utils/builtins.h" /* TPDB - KATERINA */

// used for Datum/Int comparison: int(t1)>int(t2)
static int cmp_tmsp(Datum t1, Datum t2) {
	DB printf("t1 = %lu -- t2 = %lu -- ", DatumGetInt64(t1), DatumGetInt64(t2));
	DB printf("t1 > t2 = %lu -- ", DatumGetInt64(t1) > DatumGetInt64(t2));
	DB printf("t1 < t2 = %lu \n", DatumGetInt64(t1) < DatumGetInt64(t2));
	DB printf("t1 - t2 = %lu \n", DatumGetInt64(t1) - DatumGetInt64(t2));

	return (DatumGetInt64(t1) > DatumGetInt64(t2) ) - (DatumGetInt64(t1) < DatumGetInt64(t2) );
}

// This function distinguishes between the type of adjustment to be performed 
// and executes ExecInitAdjustment
TimeAdjustmentState *
ExecInitAdjustmentWrapper(Plan *node, EState *estate, int eflags) {

	TimeIntervalAdjustment *timeIntervalAdjustment = (TimeIntervalAdjustment *) node;
	printf("WHAT NODE IS IT?\n");

	if (IsA(node, Normalization)) {
		timeIntervalAdjustment->type = 5; // Set Operations
	} else if (IsA(node, Alignment)) {
		timeIntervalAdjustment->type = 2; // Alignment
	} else if (IsA(node, NegatingWindows)) {
		timeIntervalAdjustment->type = 6; // Joins
	}

	return ExecInitAdjustment((TimeIntervalAdjustment*) node, estate, eflags);
}

// Before the ExecAdjustment is executed some attributes need to be initialized
TimeAdjustmentState *
ExecInitAdjustment(TimeIntervalAdjustment *node, EState *estate, int eflags) {
	TimeAdjustmentState *state;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	state = makeNode(TimeAdjustmentState);
	state->ss.ps.plan = (Plan *) node;
	state->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * Alignment nodes have no ExprContext initialization because they never call
	 * ExecQual or ExecProject.  But they do need a per-tuple memory context
	 * anyway for calling execTuplesMatch.
	 */
	state->tempContext = AllocSetContextCreate(CurrentMemoryContext, "Adjust", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * tuple table initialization
	 */
	ExecInitScanTupleSlot(estate, &state->ss);
	ExecInitResultTupleSlot(estate, &state->ss.ps);

	/*
	 * then initialize outer plan
	 */
	outerPlanState (state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize source tuple type.
	 */
	ExecAssignScanTypeFromOuterPlan(&state->ss);

	/*
	 * Init per left tuple sweep line
	 */
	state->sweep = 0;

	/*
	 * Alignment nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&state->ss.ps);
	state->ss.ps.ps_ProjInfo = NULL;

	state->ss.ps.ps_TupFromTlist = false;

	 printf("TYPE =  %d\n", node->type);

	if (node->type == 1 || node->type == 5) /* NORMALIZATION */
	{
		printf("PREPARE NORMALIZATION NODES --- %d\n\n", node->numCols);
		state->tagpos = node->numCols - 6;
		state->tspos = node->numCols - 5;
		state->tepos = node->numCols - 4;
		state->l1pos = node->numCols - 3;
		state->l2pos = node->numCols - 2;
		state->p1pos = node->numCols - 1;
		state->p2pos = node->numCols;
		state->alignment = false;

	} else if (node->type == 6) /* NEGATING */
		{
		state->numOfRightCols = node->numOfRightCols; // works only for relations with 2 fact attributes
		state->rightFactEndPos = node->numCols - 4;
		state->tspos = node->numCols - 3;
		state->tepos = node->numCols - 2;
		state->l1pos = node->numCols - 1;
		state->l2pos = node->numCols;
		state->windowsToBeProduced = NEGATING_JOINS;

		state->alignment = true;

	} else 
		{
		state->numOfRightCols = node->numOfRightCols; // works only for relations with 2 fact attributes
		state->rightFactEndPos = node->numCols - 6;
		state->tspos = node->numCols - 5;
		state->tepos = node->numCols - 4;
		state->l1pos = node->numCols - 3;
		state->l2pos = node->numCols - 2;
		state->p1pos = node->numCols - 1;
		state->p2pos = node->numCols;
		state->windowsToBeProduced = OVERLAPPING_UNMATCHED_JOINS;
		state->alignment = true;
	}

	/*
	 * Init masks
	 */
	state->nullMask = palloc0(sizeof(bool) * node->numCols);
	state->tsteMask = palloc0(sizeof(bool) * node->numCols);
	state->tsteMask[state->tspos - 1] = true;
	state->tsteMask[state->tepos - 1] = true;
	state->tsteMask[state->l2pos - 1] = true;
	state->tsteMask[state->l1pos - 1] = true;

	/*
	 * Init buffer values for heap_modify_tuple
	 */
	state->tupvalbuf = palloc0(sizeof(Datum) * node->numCols);

	/*
	 * Precompute fmgr lookup data for inner loop
	 */
	state->eqfunctions = execTuplesMatchPrepare(node->numCols, node->eqOperators);

	state->firstCall = true;
	state->done = false;

	state->sTuple = NULL;
	state->rTuple = NULL;

	return state;

}


TupleTableSlot *
ExecAdjustment(TimeAdjustmentState *node) {

	DB printf("Enter Exec_Adjustment\n");

	if (node == NULL ) {
		return NULL ;
	}

	Alignment *plannode = (Alignment *) node->ss.ps.plan; /* TODO make new */
	PlanState *outerPlan = outerPlanState(node);
	TupleTableSlot *resultTupleSlot = node->ss.ps.ps_ResultTupleSlot;
	TupleTableSlot *upcoming = outerPlan->ps_ResultTupleSlot;
	TupleTableSlot *outerslot = outerPlan->ps_ResultTupleSlot;
	TupleTableSlot *cur_group = node->ss.ss_ScanTupleSlot;
	TupleTableSlot *prev = node->ss.ss_ScanTupleSlot;

	HeapTuple t, u;
	bool isNull, produced;
	MemoryContext oldContext;
	Datum p1;

	if (node->done) {
		ExecClearTuple(resultTupleSlot);
		return NULL ;
	}


	
	if (node->alignment == false) { // SET OPERATIONS

		// First Time calling this function
		if (node->firstCall) {
			
			//Get a tuple - The first time
			upcoming = ExecProcNode(outerPlan);
			if (TupIsNull(upcoming)) { // Empty tupleset / No tuples
				node->done = true;
				return NULL ;
			}
	
			node->firstCall = false;
			ExecCopySlot(cur_group, upcoming); // It's the first tuple so cur_group = upcoming
	
			// Initialize variables
			node->sweep = slot_getattr(upcoming, node->tspos, &isNull); // we have just started processing a new group of tuples
			node->breakPoint = slot_getattr(upcoming, node->tspos, &isNull);
			node->sameGroup = true;
	
			DS	printf("(FirstCall) [sameGroup = %d -- -- sweep: %d --  break: %d]  \n", DatumGetInt32(node->sameGroup), DatumGetInt32(node->sweep),
				DatumGetInt32(node->breakPoint) );
	
			// Update datumCouples
			node->sTuple = NULL;
			node->rTuple = NULL;
	
			DS printf("First Call:\n");
			bool used = updateTuples(node, upcoming);
			if (used)
				ExecClearTuple(upcoming);
			DS printTuple("upc", node, upcoming);
	
		}
		
		DS1 printf("--------- (A) --------\n");
		DS1 printCellwith_datumCouple("s", node->sTuple);
		DS1 printCellwith_datumCouple("r", node->rTuple);
	
	
		bool endOfTupleProduction = false;
		while (!endOfTupleProduction) {
	
			if (cmp_tmsp(node->breakPoint, node->sweep) > 0) {
	
				DS printf("\n(1) [sameGroup = %d -- sweep: %d --  break: %d]  \n", DatumGetInt32(node->sameGroup), DatumGetInt32(node->sweep),
						DatumGetInt32(node->breakPoint) );
	
				// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% ATTRIBUTES FOR THE NEW TUPLE %%%%%%%%%%%%%%%%%%%%%%%%%%%
				char * l1_or = calloc(1000, sizeof(char));
				strcpy(l1_or, "");
				l1_or = get_lineage(node->rTuple); // Get final expression for l1
				DS printf("L1: %s\n", l1_or);
				Datum l1 = DirectFunctionCall1(textin, CStringGetDatum(l1_or));
				free(l1_or);
	
				char * l2_or = calloc(1000, sizeof(char));
				strcpy(l2_or, "");
				l2_or = get_lineage(node->sTuple); // Get final expression for l1
				DS printf("L2: %s\n", l2_or);
				Datum l2 = DirectFunctionCall1(textin, CStringGetDatum(l2_or));
				free(l2_or);
	
				// Storing the new attributes
				node->tupvalbuf[node->l1pos - 1] = l1;
				node->tupvalbuf[node->l2pos - 1] = l2;
				node->tupvalbuf[node->tspos - 1] = node->sweep;
				node->tupvalbuf[node->tepos - 1] = node->breakPoint;
	
				// %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% STORING THE NEW TUPLE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
				oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
				u = ExecCopySlotTuple(cur_group);
				t = heap_modify_tuple(u, cur_group->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
				heap_freetuple(u);
				MemoryContextSwitchTo(oldContext);
				ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true );
				endOfTupleProduction = true;
	
				deleteTuples(node);
	
				if (node->rTuple == NULL && node->sTuple == NULL ) {
					if (!TupIsNull(upcoming)) {
						node->sweep = slot_getattr(upcoming, node->tspos, &isNull); // we have just started processing a new group of tuples
						node->breakPoint = slot_getattr(upcoming, node->tspos, &isNull);	
	
						bool used = updateTuples(node, upcoming);
						DS printf("--------- (A') --------\n");
						DS printCellwith_datumCouple("s", node->sTuple);
						DS printCellwith_datumCouple("r", node->rTuple);
		
						if (!node->sameGroup) {
							node->sameGroup = true;
							ExecCopySlot(cur_group, upcoming);
						}
	
						if (used) {
							ExecClearTuple(upcoming);	
						}
	
					}
				} else {
					node->sweep = node->breakPoint;
				}
	
			} else {
				if (TupIsNull(upcoming) && node->sameGroup) {
						DS printf("--------- (B) --------\n");
						DS printCellwith_datumCouple("s", node->sTuple);
						DS printCellwith_datumCouple("r", node->rTuple);
	
						upcoming = ExecProcNode(outerPlan);

						if (!TupIsNull(upcoming)) {
							DS printTuple("upc", node, upcoming);
							DS printf("--------- (B') --------\n");
							DS printCellwith_datumCouple("s", node->sTuple);
							DS printCellwith_datumCouple("r", node->rTuple);
						} else {
							DS printf("UPC/PRINT -- %d\n", TupIsNull(upcoming));
						}
						node->sameGroup = !TupIsNull(upcoming)
								&& execTuplesMatch(upcoming, cur_group, node->tagpos - 1, plannode->ColIdx, node->eqfunctions, node->tempContext);	
				}
	
				if (!TupIsNull(upcoming) && cmp_tmsp(slot_getattr(upcoming, node->tspos, &isNull), node->sweep) > 0 && node->sameGroup) {
					node->breakPoint = minimumOf(node, slot_getattr(upcoming, node->tspos, &isNull));
				} else {
					bool used = FALSE;
					if (node->sameGroup) {
						used = updateTuples(node, upcoming);
						DS printf("--------- (C) --------\n");
						DS printCellwith_datumCouple("s", node->sTuple);
						DS printCellwith_datumCouple("r", node->rTuple);
	
						if (used)
							ExecClearTuple(upcoming);
					}
					if (!used)
						node->breakPoint = minimumOf(node, NULL );
	
				}
			}
		}
	
		if (TupIsNull(upcoming) && node->rTuple == NULL && node->sTuple == NULL ) {
			node->done = true;
		}
	

	}
	else if (node->windowsToBeProduced == OVERLAPPING_UNMATCHED_JOINS) {
		
		D1 printf("MAKING OVERLAPPING WINDOWS --- %d\n\n", node->windowsToBeProduced);
		if(node->firstCall) {
			outerslot = ExecProcNode(outerPlan);
			D1 printTuple("OUTERSLOT", node, outerslot);

			if(TupIsNull(outerslot))
			{
				node->done = true;
				return NULL;
			}
			ExecCopySlot(prev, outerslot);
			D1 printTuple("PREV", node, prev);

			node->sameGroup = true;
			node->sweep = slot_getattr(outerslot, node->tspos, &isNull);
			node->firstCall = false;
		}
		
		bool endOfTupleProduction = false;
		while (!endOfTupleProduction && !TupIsNull(prev)) {
			D1 printf("(1 -- SAME GROUP? = %d)\n", node->sameGroup);

			if(node->sameGroup) {
				p1 = slot_getattr(outerslot, node->p1pos, &isNull);
  				// (1) sweepline < overlap_start
				if(!isNull && cmp_tmsp(node->sweep, p1) < 0 && cmp_tmsp(node->sweep, slot_getattr(outerslot, node->tspos, &isNull)) >= 0){
					D1 printf("Enter - (1) - sweepline < p1\n");
					D1 printf("[ts,te) = [%llu, %llu)  \n", DatumGetInt64(node->sweep), DatumGetInt64(p1));

					char * l1 = calloc(100, sizeof(char));
					l1 = DirectFunctionCall1(textout, slot_getattr(outerslot, node->l1pos, &isNull));
					strcat(l1,"");

					node->tupvalbuf[node->l1pos - 1] = DirectFunctionCall1(textin, l1);
					node->tupvalbuf[node->tspos-1] = node->sweep;
					node->tupvalbuf[node->tepos-1] = p1;

					// // printf("MAKE NULL - (1)\n");
  					nullifyFact(node);
					node->nullMask[node->l2pos-1] = true;


					oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
					u = ExecCopySlotTuple(outerslot);
					t = heap_modify_tuple(u, outerslot->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
					heap_freetuple(u);
					MemoryContextSwitchTo(oldContext);
					ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true);

					deNullifyFact(node);
					node->nullMask[node->l2pos-1] = false;

					node->sweep = p1;
					endOfTupleProduction = true;
				}
				else if (slot_getattr(outerslot, node->l1pos, &isNull) != NULL
					&& slot_getattr(outerslot, node->l2pos, &isNull) != NULL){

					D1 printf("Enter - (2) - OVERLAP AND align\n");
					D1 printf("[ts,te) = [%lu,", DatumGetInt64(slot_getattr(outerslot, node->p1pos, &isNull)));
					D1 printf("%lu)  \n", DatumGetInt64(slot_getattr(outerslot, node->p2pos, &isNull)));

					// Assign Time Intervals
					node->tupvalbuf[node->tspos-1] = slot_getattr(outerslot, node->p1pos, &isNull);
					node->tupvalbuf[node->tepos-1] = slot_getattr(outerslot, node->p2pos, &isNull);

					// Make Strings For Lineages L1
					if (slot_getattr(outerslot, node->l1pos, &isNull) == NULL) {
					} else {
					}
					char * l1 = calloc(100, sizeof(char));
					l1 = DirectFunctionCall1(textout, slot_getattr(outerslot, node->l1pos, &isNull));
					
					Datum l1_dat = DirectFunctionCall1(textin, l1);
					node->tupvalbuf[node->l1pos-1] =l1_dat;
	
					// Make String for Lineage L2
					Datum l2_dat = slot_getattr(outerslot, node->l2pos, &isNull);
					char * l2 = calloc(100, sizeof(char));

					if (!isNull) {
						l2 = DirectFunctionCall1(textout, l2_dat);
						node->tupvalbuf[node->l2pos-1] = DirectFunctionCall1(textin, l2);
					} else {
						node->nullMask[node->l2pos-1] = true;
					}
					
					// Store New Attributes to an output tuple
					oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
					u = ExecCopySlotTuple(outerslot);
					t = heap_modify_tuple(u, outerslot->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
					heap_freetuple(u);
					ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true);
	

					node->nullMask[node->l2pos-1] = false;
					
					// Determine new sweepline position	
					if (cmp_tmsp(node->sweep, slot_getattr(outerslot, node->p2pos, &isNull)) < 0) {
						node->sweep = slot_getattr(outerslot, node->p2pos, &isNull);
						D1 printf("SWEEP UPDATED = %lu\n", DatumGetInt64(node->sweep));
					}
  						
					

					endOfTupleProduction = true;
					

  					
	
					ExecCopySlot(prev, outerslot);
					outerslot = ExecProcNode(outerPlan);
					D1 printTuple("OUTERSLOT", node, outerslot);
					D1 printTuple("PREV", node, prev);

					/* *********** Check same group from rightFatEndPos to numOfRightCols ******/
					if (!TupIsNull(outerslot)) {
						node->sameGroup = execTuplesMatchFacts(node, plannode, prev, outerslot,0);
						D1 printf("(SAME GROUP? = %d)\n", node->sameGroup);
						// Condition to avoid sending empty windows to the output
						if(node->sameGroup && cmp_tmsp(node->sweep, slot_getattr(prev, node->tepos, &isNull)) == 0) {
							node->sweep = slot_getattr(outerslot, node->p1pos, &isNull);
						}
							
					} else {
						node->sameGroup = false;
					}
						
				}
				else if (slot_getattr(outerslot, node->l2pos, &isNull) == NULL){
  					D1 printf("NO RIGHT LINEAGE\n");
					D1 printf("[ts,te) = [%lu,", DatumGetInt64(slot_getattr(outerslot, node->tspos, &isNull)));
					D1 printf("%lu)  \n", DatumGetInt64(slot_getattr(outerslot, node->tepos, &isNull)));

					oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
					u = ExecCopySlotTuple(outerslot);
					MemoryContextSwitchTo(oldContext);
					ExecStoreTuple(u, resultTupleSlot, InvalidBuffer, true );
					endOfTupleProduction = true;

					ExecCopySlot(prev, outerslot);
					outerslot = ExecProcNode(outerPlan);
					D1 printTuple("OUTERSLOT", node, outerslot);
					D1 printTuple("PREV", node, prev);
	
					if (!TupIsNull(outerslot)) {
						node->sameGroup = true;
						node->sweep = slot_getattr(outerslot, node->tspos, &isNull);
					} else {
						prev = ExecClearTuple(prev);
						node->done = true;
					}

				}
			}
			else {
				D1 printf("Enter - (3) - group switching \n");
				
				Datum prevTe = slot_getattr(prev, node->tepos, &isNull);

				D1 printf("[sweep,prevte) = [%lu, %lu)  \n", DatumGetInt64(node->sweep), DatumGetInt64(prevTe));

				char * l1 = calloc(100, sizeof(char));
				if(cmp_tmsp(node->sweep, prevTe) < 0) {

					l1 = DirectFunctionCall1(textout, slot_getattr(prev, node->l1pos, &isNull));
					D1 printf("Enter - (3a) - remaining from prev \n");
					D1 printf("[ts,te) = [%llu, %llu)  \n", DatumGetInt64(node->sweep), DatumGetInt64(prevTe));

					node->tupvalbuf[node->l1pos-1] = DirectFunctionCall1(textin, l1);
					node->tupvalbuf[node->tspos-1] = node->sweep;
					node->tupvalbuf[node->tepos-1] = prevTe;

  					nullifyFact(node);
					node->nullMask[node->l2pos-1] = true;

					oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
					t = heap_modify_tuple(prev->tts_tuple, prev->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
					MemoryContextSwitchTo(oldContext);
					ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true);
					endOfTupleProduction = true;

  					deNullifyFact(node);
					node->nullMask[node->l2pos-1] = false;

				} 


				D1 printf("Enter - (3b) - finalizing group switch\n");
				ExecCopySlot(prev, outerslot);	

				if(!TupIsNull(outerslot)) {  	
					D1 printf("BEF: [ts,te) = [%lu,", DatumGetInt64(slot_getattr(outerslot, node->tspos, &isNull)));
					D1 printf("%lu)  \n", DatumGetInt64(slot_getattr(outerslot, node->tepos, &isNull)));
				
					node->sweep = slot_getattr(outerslot, node->tspos, &isNull);
					
					D1 printf("BEF: [ts,te) = [%lu,", DatumGetInt64(slot_getattr(outerslot, node->tspos, &isNull)));
					D1 printf("%lu)  \n", DatumGetInt64(slot_getattr(outerslot, node->tepos, &isNull)));

					node->sameGroup = true;
				} else {
					prev = ExecClearTuple(prev);
					D1 printf("CLEAR PREV\n");
				}
			}
		
		}
		


		if(!endOfTupleProduction) {
			ExecClearTuple(resultTupleSlot);
			node->done = true;
			printf("done = %d\n", node->done);
		}

		D1 printf("TUPLE IS READY - UO\n");
	}

	else if (node->windowsToBeProduced = NEGATING_JOINS) {


			D printf("\n\n\nMAKING NEGATING WINDOWS --- %d -- NEGATING = %d\n\n", node->windowsToBeProduced, node->negating);


			if (node->firstCall) {

				D printf("First Call:\n");

				//Get a tuple - The first time
				upcoming = ExecProcNode(outerPlan);
				
				D printTuple("UPCOMING", node, upcoming);

				
				// node->PQ = new_list(T_List);
				if (TupIsNull(upcoming)) { // Empty tupleset / No tuples
					node->done = true;
					return NULL ;
				}

				node->firstCall = false;
				// ExecCopySlot(cur_group, upcoming); // It's the first tuple so cur_group = upcoming

				node->prevWindTe = -1;
				node->negating = false;

			}

			D printf("[prevWindTe: %ld]  \n", DatumGetInt64(node->prevWindTe));



			if (node->prevWindTe == -1 && slot_getattr(upcoming, node->l2pos, &isNull) != NULL) {
				D printf("NEGATING: new group\n");
				ExecCopySlot(cur_group, upcoming);
				node->prevWindTe = slot_getattr(upcoming, node->tspos, &isNull); 

				node->PQ = NIL;
	
				D printTuple("CUR_GROUP", node, cur_group);

			}			

			bool endOfTupleProduction = false;
			while (!endOfTupleProduction) {
				if (node->negating == false) {
					endOfTupleProduction = true;
					oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
					u = ExecCopySlotTuple(upcoming);
					MemoryContextSwitchTo(oldContext);
					ExecStoreTuple(u, resultTupleSlot, InvalidBuffer, true );

					if (slot_getattr(upcoming, node->l2pos, &isNull) == NULL) {
						D printf("L2 IS NULL\n");
						node->prevWindTe = -1; 
						node->negating = false;
						upcoming = ExecProcNode(outerPlan);

						D printTuple("UPCOMING", node, upcoming);
						D printTuple("CUR_GROUP", node, cur_group);


					} else {
						D printf("L2 IS NOT NULL\n");
						timeLineagePair *toAdd = malloc(sizeof(timeLineagePair));
						toAdd->te = slot_getattr(upcoming, node->tepos, &isNull);

						char * toAddL1 = calloc(100, sizeof(char));
						toAddL1 = DirectFunctionCall1(textout, slot_getattr(upcoming, node->l2pos, &isNull));

						toAdd->lin1 = DirectFunctionCall1(textin, toAddL1); // keeping l2 would be the same


						/* ********************** Add nodel to list ********************** */
						ListCell *now, *bef = NULL;
						bool added = false;
	
						if (node->PQ == NIL) {
							DL printf("LIST IS EMPTY\n");
							DL printf("[IN EMPTY] node = (%ld, %s)\n", DatumGetInt64(toAdd->te), 
																	  DirectFunctionCall1(textout, toAdd->lin1));
							node->PQ = lappend(node->PQ, toAdd);
							// return;
						} else {
							DL printf("LIST IS NOT EMPTY --- BEFORE\n");
							DL printf("#Elements = %d\n", list_length(node->PQ));


							foreach(now, node->PQ) {
								Datum te_from_now = ((timeLineagePair *) lfirst(now))->te;
								if (cmp_tmsp(te_from_now, toAdd->te) > 0) {

									if (bef == NULL) {
										node->PQ = lprepend(node->PQ, toAdd);

									} else {
										lappend_cell(node->PQ, bef, toAdd);	
									}
									
									
									added = true;
									break;
								} else {
									bef = now;
								}
							}
							if (!added) {
								node->PQ = lappend(node->PQ, toAdd);
							} 

						}
						DL printf("ADDED TO LIST  --- AFFTER\n");
						D printList(node->PQ);
	
						node->negating = true;

					}

				} else {
					D printf("NEGATING: New Negating Tuple\n");

					/* *********** Check same group from rightFatEndPos to numOfRightCols ******/
					if (!TupIsNull(upcoming)) {
						node->sameGroup = execTuplesMatchFacts(node, plannode, upcoming, cur_group,1);
					} else node->sameGroup = false;
					
					if (!TupIsNull(upcoming)) {
						D printf("[ts = %ld -- -- prevWindTe: %ld]  \n",
						  DatumGetInt64(slot_getattr(upcoming, node->tspos, &isNull)),
						  DatumGetInt64(node->prevWindTe));
					} else {

					}

					if (node->sameGroup &&
						cmp_tmsp(slot_getattr(upcoming, node->tspos, &isNull), node->prevWindTe) <=0) {
						upcoming = ExecProcNode(outerPlan);

						D printTuple("UPCOMING", node, upcoming);
						D printTuple("CUR_GROUP", node, cur_group);


					}
				}
			
				if (!endOfTupleProduction) {

					int counter = 0;

					/* *********** Check same group from rightFatEndPos to numOfRightCols ******/
					if (!TupIsNull(upcoming) && !TupIsNull(cur_group)) {
						D printf("COMPARISON --- LINE 17\n");
						node->sameGroup = execTuplesMatchFacts(node, plannode, upcoming, cur_group,1);
					} else node->sameGroup = false;					

					// lines 19-24
					D printf("COMPARING: TS, PREVWINDTE\n");

					if (node->sameGroup && 
						cmp_tmsp(slot_getattr(upcoming, node->tspos, &isNull), node->prevWindTe) >0) {
						Datum windTs = slot_getattr(upcoming, node->tspos, &isNull);
						Datum windTe = ((timeLineagePair *) linitial(node->PQ))->te;
						
						D printf("COMPARING TS, TE --- ");
						D printf("[ts = %ld -- -- te: %ld]  \n", DatumGetInt64(windTs), DatumGetInt64(windTe));


						if (cmp_tmsp(windTe, windTs) >0) {
	
							// Negating tuple initial step
							D printf("NEGATING: beginning of interval\n");
	
							windTe = windTs;
							
							// Traverse the list and disjunct the lineages
							char *l2 = lineage_disjunction(node);
	
							
							char * l1 = calloc(100, sizeof(char));
							l1 = DirectFunctionCall1(textout, slot_getattr(upcoming, node->l1pos, &isNull));
	
							node->tupvalbuf[node->l1pos-1] = DirectFunctionCall1(textin, l1);
							node->tupvalbuf[node->l2pos-1] = DirectFunctionCall1(textin, l2);
							node->tupvalbuf[node->tspos-1] = node->prevWindTe;
							node->tupvalbuf[node->tepos-1] = windTe;
	
							D printf("[OUTPUT] [ts,te) = [%llu, %llu)  \n", DatumGetInt64(node->prevWindTe), DatumGetInt64(windTe));

							nullifyFact(node);

	
							oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
							t = heap_modify_tuple(prev->tts_tuple, prev->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
							MemoryContextSwitchTo(oldContext);
							ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true);
							endOfTupleProduction = true;
	
							deNullifyFact(node);
							
							node->negating = false;
							node->prevWindTe = windTe;

							D printf("[prevWindTe: %ld]  \n", DatumGetInt64(node->prevWindTe));

							endOfTupleProduction = true;
							break;

						}
					} 
					else if (node->sameGroup && cmp_tmsp(slot_getattr(upcoming, node->tspos, &isNull), node->prevWindTe) == 0) {
						node->negating = false;
						continue;
					} 
					if (node->PQ != NIL) {
						D printf("\n\nTHERE ARE MORE THINGS IN THE LIST\n\n");

						Datum windTe = ((timeLineagePair *) linitial(node->PQ))->te;
	
						// Traverse the list and disjunct the lineages
						char *l2 = lineage_disjunction(node);	
						
						char * l1 = calloc(100, sizeof(char));
						l1 = DirectFunctionCall1(textout, slot_getattr(cur_group, node->l1pos, &isNull));
	
						node->tupvalbuf[node->l1pos-1] = DirectFunctionCall1(textin, l1);
						node->tupvalbuf[node->l2pos-1] = DirectFunctionCall1(textin, l2);
						node->tupvalbuf[node->tspos-1] = node->prevWindTe;
						node->tupvalbuf[node->tepos-1] = windTe;
	
	
						D printf("[OUTPUT] [ts,te) = [%llu, %llu)  \n", DatumGetInt64(node->prevWindTe), DatumGetInt64(windTe));

						nullifyFact(node);
	
						oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
						t = heap_modify_tuple(prev->tts_tuple, prev->tts_tupleDescriptor, node->tupvalbuf, node->nullMask, node->tsteMask);
						MemoryContextSwitchTo(oldContext);
						ExecStoreTuple(t, resultTupleSlot, InvalidBuffer, true);
						endOfTupleProduction = true;
	
						deNullifyFact(node);

						node->negating = true;
						node->prevWindTe = windTe;

						D printf("[prevWindTe: %ld]  \n", DatumGetInt64(node->prevWindTe));

						/* ********************** Delete nodel to list ********************** */
						ListCell *now, *bef = NIL;
	
						if (node->PQ == NIL) {
							break;
						} else {

							/* ********************** Delete node to list ********************** */
							DL printf("CHECK LIST AGAIN BY LENGTH = %ld -- AND DELETE\n", node->PQ->length);


							timeLineagePair *tlPair;
							ListCell *now, *prev = NIL;

							foreach(now, node->PQ) {

								tlPair = (timeLineagePair*) lfirst(now);

								if (cmp_tmsp(tlPair->te, node->prevWindTe)<=0) {

									node->PQ->length--;

									if (prev) {
										prev->next = now->next;
									}
									else {
										node->PQ->head = now->next;
									}

									pfree(now);

								} else {
									break;
								}
							}

							/* ********************** Delete node to list ********************** */
						}

						if (node->PQ->length == 0) {
							list_free(node->PQ);
							node->PQ = NIL;
							break;
						}

					}
				}
			}
			


		// // ***********************************************************************************************************//
		// // USE IT TO CHECK THE RESULT OF THE SORTED UNION
		// upcoming = ExecProcNode(outerPlan);
		
		// if (TupIsNull(upcoming)) {
		// 	// printf("No tuple produced\n");
		// 	node->done = true;
		// 	//ExecClearTuple(resultTupleSlot);
		// } else {
		
		// 	node->sweep = slot_getattr(upcoming, node->tspos, &isNull); // we have just started processing a new group of tuples
		// 	node->breakPoint = slot_getattr(upcoming, node->tepos, &isNull);
		// 	node->sameGroup = true;
		
		// 	printf("(TEST) [sameGroup = %d -- -- sweep: %d --  break: %d]\n", DatumGetInt32(node->sameGroup), DatumGetInt32(node->sweep),
		// 	DatumGetInt32(node->breakPoint) );
		
		
		// 	ExecCopySlot(cur_group, upcoming);
		// 	oldContext = MemoryContextSwitchTo(node->ss.ps.ps_ResultTupleSlot->tts_mcxt);
		// 	u = ExecCopySlotTuple(cur_group);
		// 	MemoryContextSwitchTo(oldContext);
		// 	ExecStoreTuple(u, resultTupleSlot, InvalidBuffer, true );
		// 	node->firstCall = false;
		// }
	
		// // USE IT TO CHECK THE RESULT OF THE SORTED UNION
		// // ***********************************************************************************************************//

	
		D printf("IS THE LIST NULL? - CHECK\n");
		if (node->PQ == NIL || list_length(node->PQ) == 0) {

			D printf("IS THE LIST NULL?\n");
			if (TupIsNull(upcoming)) {
				D printf("WHY IS IT NULL?\n");
				node->done = true;
			}
			else {
				D printf("HERE?\n");
				node->prevWindTe = -1;
				node->negating = false;
			}
		}

		D printf("TUPLE IS READY - N --- [prevWindTe: %ld] ----------------------------- \n", DatumGetInt64(node->prevWindTe));


	}
	
	

	D printTupleOverlapNegate(node, resultTupleSlot);

	return resultTupleSlot;
}

// returns the lineage attribute of a tuple
char* get_lineage(datumCouple *rsTuple) {

	char* l_final = calloc(100, sizeof(char));
	strcpy(l_final, "");

	if (rsTuple == NULL ) {
		return l_final;
	}

	char* lcur = calloc(100, sizeof(char));
	lcur = DirectFunctionCall1(textout, rsTuple->objC);
	strcat(l_final, lcur);
	return l_final;

}



/*
 * nullifyFact -
 *	  makes the attributes of the right fact of a tuple NULL
 */
void nullifyFact(TimeAdjustmentState *node) {
	int counter = 0;
	for (counter = 0; counter < node->numOfRightCols; counter++) {
		node->tsteMask[node->rightFactEndPos-counter-1] = true;
		node->nullMask[node->rightFactEndPos-counter-1] = true;
	}
}

/*
 * nullifyFact -
 *	  makes the attributes of the right fact of a tuple non-NULL
 */
void deNullifyFact(TimeAdjustmentState *node) {
	int counter = 0;
	for (counter = 0; counter < node->numOfRightCols; counter++) {
		node->tsteMask[node->rightFactEndPos-counter-1] = false;
		node->nullMask[node->rightFactEndPos-counter-1] = false;
	}
}



/*
 * updateTuples -
 *	  updates node by recording in node->rTuple or node->sTuple the
 *    tuple test, based on which relation it belongs to (used when
 * 	  performing adjustment for set operations
 */
bool updateTuples(TimeAdjustmentState *node, TupleTableSlot *test) {

	TupleTableSlot *upcoming = node->ss.ss_ScanTupleSlot;
	ExecCopySlot(upcoming, test);

	// Add Elemement to list based on its tag
	bool isNull;
	Datum tag = slot_getattr(upcoming, node->tagpos, &isNull);

	datumCouple* toAdd = malloc(sizeof(datumCouple));
	toAdd->objA = slot_getattr(upcoming, node->tspos, &isNull);
	toAdd->objB = slot_getattr(upcoming, node->tepos, &isNull);
	toAdd->objC = slot_getattr(upcoming, node->l1pos, &isNull); // keeping l2 would be the same

	if (DatumGetInt64(tag) == 0) {
		if (node->rTuple == NULL ) {
			node->rTuple = toAdd;
		} else {
			return false ;
		}
	} else if (DatumGetInt64(tag) == 1) {
		if (node->sTuple == NULL ) {
			node->sTuple = toAdd;
		} else {
			return false ;
		}
	}

	return true ;
}


/*
 * deleteTuples -
 *	  makes node->rTuple or node->sTuple NULL depending on whether they have stopped being
 *	  valid, i.e. when their te has proceeds or is equal to the breakpoint (sweeping point)
 */
void deleteTuples(TimeAdjustmentState *node) {

	if (node->sTuple != NULL && cmp_tmsp(node->sTuple->objB, node->breakPoint) <= 0) {
		node->sTuple = NULL;
	}
	if (node->rTuple != NULL && cmp_tmsp(node->rTuple->objB, node->breakPoint) <= 0) {
		node->rTuple = NULL;
	}
	return;

}

/*
 * minimumOf -
 *	  computes the minimum among tsDatum, node->rTuple->objB or node->sTuple->objB
 */
Datum minimumOf(TimeAdjustmentState *node, Datum tsDatum) {

	Datum min = NULL;

	if (node->sTuple != NULL ) {
		min = node->sTuple->objB;
	} 

	if (node->rTuple != NULL ) {
		if (min == NULL || (min != NULL && cmp_tmsp(min, node->rTuple->objB) > 0))
			min = node->rTuple->objB;
	} 
	if (tsDatum != NULL && (min == NULL || (min != NULL && cmp_tmsp(min, tsDatum) > 0))) {
		min = tsDatum;
	} 

	return min;

}


/*
 * add_lineage -
 *	  adds the pairToAdd in the proper position of the priority queue node->PQ
 */
void add_lineage(TimeAdjustmentState *node, timeLineagePair *pairToAdd) {
	bool isNull;

	ListCell *now, *bef;



	if (list_length(node->PQ) == 0) {
		node->PQ = lappend(node->PQ, pairToAdd);
		return;
	}

	foreach(now, node->PQ)
	{
		// timeLineagePair currentPair = lfirst(now)
		Datum te_from_now = ((timeLineagePair *) lfirst(now))->te;
		if (cmp_tmsp(te_from_now, pairToAdd->te) < 0) {
			node->PQ = lappend_cell(node->PQ, bef, pairToAdd);
			break;
		} else
		bef = now;

	}

	return;
}

/*
 * lineage_disjunction -
 *	  combines with disjunction the lineages of all the pairs in the priority queue node->PQ
 */
char* lineage_disjunction(TimeAdjustmentState *node) {

	int lenLor = list_length(node->PQ);
	char* lor = calloc(100 * lenLor, sizeof(char));
	strcpy(lor, "");

	if (lenLor < 1) {
		return lor;
	}

	if (lenLor > 1)
		strcpy(lor, "(");

	ListCell *lc;
	foreach(lc, node->PQ)
	{
		char* lcur = calloc(lenLor, sizeof(char));
		lcur = DirectFunctionCall1(textout, ((timeLineagePair *) lfirst(lc))->lin1);
		strcat(lor, lcur);
		if (lnext(lc) != NIL )
			strcat(lor, " + ");
	}
	if (lenLor > 1)
		strcat(lor, ")");
	return lor;

}


void ExecEndAdjustment(TimeAdjustmentState *node) {
	/* clean up tuple table */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	MemoryContextDelete(node->tempContext);

	ExecEndNode(outerPlanState(node) );
}


/*
 * printList -
 *	  prints priority queue node->PQ
 */
void printList(List* sList) {
// Print the list after concat
	ListCell *p;
	printf("Just Print the List:");
	foreach(p,sList)
	{

		char* lcur = calloc(200, sizeof(char));
		lcur = DirectFunctionCall1(textout, ((timeLineagePair *) lfirst(p))->lin1);

		printf("(%ld, %s) - ", DatumGetInt64(((timeLineagePair *) lfirst(p))->te), 
			lcur);
	}
	printf("\n");
	return;
}

/*
 * printTuple -
 *	  prints the tuple print_tuple based on its schema found in node 
 */
void printTuple(char* tupleTyple, TimeAdjustmentState *node, TupleTableSlot * print_tuple) {
	bool isNull;

	if (TupIsNull(print_tuple)) {
		return;
	}

	DB printf("[%s] ts: %llu -- te: %llu",
	tupleTyple,
	DatumGetInt64(slot_getattr(print_tuple, node->tspos, &isNull)),
	DatumGetInt64(slot_getattr(print_tuple, node->tepos, &isNull)));

	Datum l1 = slot_getattr(print_tuple, node->l1pos, &isNull);
	Datum l2 = slot_getattr(print_tuple, node->l2pos, &isNull);
	if (l2 != NULL) {
		DB printf(" -- l1: %s -- l2:%s\n", DirectFunctionCall1(textout, DatumGetCString(l1)),
		 								   DirectFunctionCall1(textout, DatumGetCString(l2)));
	} else {
		printf(" -- l1: %s -- l2: NULL\n",		
		DirectFunctionCall1(textout, DatumGetCString(l1)));
	}

	return;
}

/*
 * printCellwith_datumCouple -
 *	  prints the datumCouple dCouple corresponding to a valid tuple of relation r
 *    or relation s depending on r_Or_s
 */
void printCellwith_datumCouple(char* r_Or_s, datumCouple *dCouple) {

	if (dCouple == NULL ) {
		printf("%s: NULL\n", r_Or_s);
	}
	else {
		 printf("%s: te = %d --- lambda = %s\n", r_Or_s, DatumGetInt64(dCouple->objB),
		 DirectFunctionCall1(textout, dCouple->objC));
	}
	return;
}

/*
 * printTupleOverlapNegate -
 *	  prints the tuple print_tuple (corresponding to a window) based on its schema found in node 
 */
void printTupleOverlapNegate(TimeAdjustmentState *node, TupleTableSlot *print_tuple) {
	bool isNull;

	printf(" ---- OUT ( ");

	if (TupIsNull(print_tuple)) {
		printf(" )  ----\n");
		return;
	}
	Datum Ts = slot_getattr(print_tuple, node->tspos, &isNull);
	Datum Te = slot_getattr(print_tuple, node->tepos, &isNull);
	Datum l1 = slot_getattr(print_tuple, node->l1pos, &isNull);
	Datum l2 = slot_getattr(print_tuple, node->l2pos, &isNull);
	printf("Ts: %d -- Te: %d -- ", DatumGetInt64(Ts), DatumGetInt64(Te));
	if (l1 != NULL) {
		printf("l1:%s -- ", DirectFunctionCall1(textout, l1));
	} else {
		printf("l1: -- ");
	}
	if (l2 != NULL) {
		printf("l2:%s", DirectFunctionCall1(textout, l2));
	} else {
		printf("l2: ");
	}

	printf(" )  ----\n");

	return;
}