/*
 * nodeTimeAdjustment.h
 * TPDB - KATERINA
 */

#ifndef NODETIMEADJUSTMENT_H_
#define NODETIMEADJUSTMENT_H_

#include "nodes/execnodes.h"

extern TimeAdjustmentState *ExecInitAdjustmentWrapper(Plan *node, EState *estate, int eflags);
extern TimeAdjustmentState *ExecInitAdjustment(TimeIntervalAdjustment *node, EState *estate, int eflags);
extern TupleTableSlot *ExecAdjustment(TimeAdjustmentState *node);
extern void ExecEndAdjustment(TimeAdjustmentState *node);

// my_functions
void printTuple(char* tupleTyple, TimeAdjustmentState *node, TupleTableSlot *print_tuple);
void printCellwith_datumCouple(char* r_Or_s, datumCouple *dCouple);

char* get_lineage(datumCouple *rsTuple);
Datum minimumOf(TimeAdjustmentState *node, Datum tsDatum);
bool updateTuples(TimeAdjustmentState *node, TupleTableSlot *cur_tup);
void deleteTuples(TimeAdjustmentState *node);

void nullifyFact(TimeAdjustmentState *node);
void deNullifyFact(TimeAdjustmentState *node);

extern void printList(List* sList);
extern char* lineage_disjunction(TimeAdjustmentState *node);
extern void add_lineage(TimeAdjustmentState *node, timeLineagePair *pairToAdd);
extern void printTupleOverlapNegate(TimeAdjustmentState *node, TupleTableSlot *print_tuple);

#endif /* NODETIMEADJUSTMENT_H_ */
