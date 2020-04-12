package edu.berkeley.cs186.database.query;

import java.lang.reflect.Array;
import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

/**
 * QueryPlan provides a set of functions to generate simple queries. Calling the methods corresponding
 * to SQL syntax stores the information in the QueryPlan, and calling execute generates and executes
 * a QueryPlan DAG.
 */
public class QueryPlan {
    private TransactionContext transaction;
    private QueryOperator finalOperator;
    private String startTableName;

    private List<String> joinTableNames;
    private List<String> joinLeftColumnNames;
    private List<String> joinRightColumnNames;
    private List<String> selectColumnNames;
    private List<PredicateOperator> selectOperators;
    private List<DataBox> selectDataBoxes;
    private List<String> projectColumns;
    private Map<String, String> aliases;
    private String groupByColumn;
    private boolean hasCount;
    private String averageColumnName;
    private String sumColumnName;

    /**
     * Creates a new QueryPlan within transaction. The base table is startTableName.
     *
     * @param transaction the transaction containing this query
     * @param startTableName the source table for this query
     */
    public QueryPlan(TransactionContext transaction, String startTableName) {
        this(transaction, startTableName, startTableName);
    }

    /**
     * Creates a new QueryPlan within transaction. The base table is startTableName,
     * aliased to aliasTableName.
     *
     * @param transaction the transaction containing this query
     * @param startTableName the source table for this query
     * @param aliasTableName the alias for the source table
     */
    public QueryPlan(TransactionContext transaction, String startTableName, String aliasTableName) {
        this.transaction = transaction;
        this.startTableName = aliasTableName;

        this.projectColumns = new ArrayList<>();
        this.joinTableNames = new ArrayList<>();
        this.joinLeftColumnNames = new ArrayList<>();
        this.joinRightColumnNames = new ArrayList<>();

        // The select lists are of the same length. See select()
        this.selectColumnNames = new ArrayList<>();
        this.selectOperators = new ArrayList<>();
        this.selectDataBoxes = new ArrayList<>();

        this.aliases = new HashMap<>();
        this.aliases.put(aliasTableName, startTableName);

        this.hasCount = false;
        this.averageColumnName = null;
        this.sumColumnName = null;

        this.groupByColumn = null;

        this.finalOperator = null;

        this.transaction.setAliasMap(this.aliases);
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }

    /**
     * Add a project operator to the QueryPlan with a list of column names. Can only specify one set
     * of projections.
     *
     * @param columnNames the columns to project
     */
    public void project(List<String> columnNames) {
        if (!this.projectColumns.isEmpty()) {
            throw new QueryPlanException("Cannot add more than one project operator to this query.");
        }

        if (columnNames.isEmpty()) {
            throw new QueryPlanException("Cannot project no columns.");
        }

        this.projectColumns = new ArrayList<>(columnNames);
    }

    /**
     * Add a select operator. Only returns columns in which the column fulfills the predicate relative
     * to value.
     *
     * @param column the column to specify the predicate on
     * @param comparison the comparator
     * @param value the value to compare against
     */
    public void select(String column, PredicateOperator comparison,
                       DataBox value) {
        this.selectColumnNames.add(column);
        this.selectOperators.add(comparison);
        this.selectDataBoxes.add(value);
    }

    /**
     * Set the group by column for this query.
     *
     * @param column the column to group by
     */
    public void groupBy(String column) {
        this.groupByColumn = column;
    }

    /**
     * Add a count aggregate to this query. Only can specify count(*).
     */
    public void count() {
        this.hasCount = true;
    }

    /**
     * Add an average on column. Can only average over integer or float columns.
     *
     * @param column the column to average
     */
    public void average(String column) {
        this.averageColumnName = column;
    }

    /**
     * Add a sum on column. Can only sum integer or float columns
     *
     * @param column the column to sum
     */
    public void sum(String column) {
        this.sumColumnName = column;
    }

    /**
     * Join the leftColumnName column of the existing queryplan against the rightColumnName column
     * of tableName.
     *
     * @param tableName the table to join against
     * @param leftColumnName the join column in the existing QueryPlan
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String leftColumnName, String rightColumnName) {
        join(tableName, tableName, leftColumnName, rightColumnName);
    }

    /**
     * Join the leftColumnName column of the existing queryplan against the rightColumnName column
     * of tableName, aliased as aliasTableName.
     *
     * @param tableName the table to join against
     * @param aliasTableName alias of table to join against
     * @param leftColumnName the join column in the existing QueryPlan
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String aliasTableName, String leftColumnName,
                     String rightColumnName) {
        if (this.aliases.containsKey(aliasTableName)) {
            throw new QueryPlanException("table/alias " + aliasTableName + " already in use");
        }
        this.joinTableNames.add(aliasTableName);
        this.aliases.put(aliasTableName, tableName);
        this.joinLeftColumnNames.add(leftColumnName);
        this.joinRightColumnNames.add(rightColumnName);
        this.transaction.setAliasMap(this.aliases);
    }

    //Returns a 2-array of table name, column name
    private String [] getJoinLeftColumnNameByIndex(int i) {
        return this.joinLeftColumnNames.get(i).split("\\.");
    }

    //Returns a 2-array of table name, column name
    private String [] getJoinRightColumnNameByIndex(int i) {
        return this.joinRightColumnNames.get(i).split("\\.");
    }

    /**
     * Generates a naive QueryPlan in which all joins are at the bottom of the DAG followed by all select
     * predicates, an optional group by operator, and a set of projects (in that order).
     *
     * @return an iterator of records that is the result of this query
     */
    public Iterator<Record> executeNaive() {
        this.transaction.setAliasMap(this.aliases);
        try {
            String indexColumn = this.checkIndexEligible();

            if (indexColumn != null) {
                this.generateIndexPlan(indexColumn);
            } else {
                // start off with the start table scan as the source
                this.finalOperator = new SequentialScanOperator(this.transaction, this.startTableName);

                this.addJoins();
                this.addSelects();
                this.addGroupBy();
                this.addProjects();
            }

            return this.finalOperator.execute();
        } finally {
            this.transaction.clearAliasMap();
        }
    }

    /**
     * Generates an optimal QueryPlan based on the System R cost-based query optimizer.
     *
     * @return an iterator of records that is the result of this query
     */
    public Iterator<Record> execute() {
        List<String> tables = new ArrayList<>();
        tables.add(startTableName);
        for (String joinTable : joinTableNames) {
            tables.add(joinTable);
        }
        int pass = 1; //Track pass
        // Pass 1: Iterate through all single tables. For each single table, find
        // the lowest cost QueryOperator to access that table. Construct a mapping
        // of each table name to its lowest cost operator.
        Map<Set, QueryOperator> tableToOp = new HashMap<>();
        for (String table: tables) {
            QueryOperator cheapest = minCostSingleAccess(table); // lowest cost query operator
            Set<String> tableSet = new HashSet<>();
            tableSet.add(table);
            tableToOp.put(tableSet, cheapest);
        }
        // Pass i: On each pass, use the results from the previous pass to find the
        // lowest cost joins with each single table. Repeat until all tables have
        // been joined.
        Map<Set, QueryOperator> m1 = tableToOp; //pass 1 map starts at first pass.
        Map<Set, QueryOperator> m2; //previous map.
        for (;pass < tables.size(); pass++) {
            m2 = tableToOp; //save (previous) map.
            tableToOp = minCostJoins(m2,m1); //get lowest cost join at pass i. (Update map)
        }
        // Get the lowest cost operator from the last pass, add GROUP BY and SELECT
        // operators, and return an iterator on the final operator
        finalOperator = minCostOperator(tableToOp);
        addGroupBy();
        addProjects(); //select ops
        return this.finalOperator.iterator(); //iterator on final operator
    }

    /**
     * Gets all SELECT predicates for which there exists an index on the column
     * referenced in that predicate for the given table.
     *
     * @return an ArrayList of SELECT predicates
     */
    private List<Integer> getEligibleIndexColumns(String table) {
        List<Integer> selectIndices = new ArrayList<>();

        for (int i = 0; i < this.selectColumnNames.size(); i++) {
            String column = this.selectColumnNames.get(i);

            if (this.transaction.indexExists(table, column) &&
                    this.selectOperators.get(i) != PredicateOperator.NOT_EQUALS) {
                selectIndices.add(i);
            }
        }

        return selectIndices;
    }

    /**
     * Gets all columns for which there exists an index for that table
     *
     * @return an ArrayList of column names
     */
    private List<String> getAllIndexColumns(String table) {
        List<String> indexColumns = new ArrayList<>();

        Schema schema = this.transaction.getSchema(table);
        List<String> columnNames = schema.getFieldNames();

        for (String column : columnNames) {
            if (this.transaction.indexExists(table, column)) {
                indexColumns.add(table + "." + column);
            }
        }

        return indexColumns;
    }

    /**
     * Applies all eligible SELECT predicates to a given source, except for the
     * predicate at index except. The purpose of except is because there might
     * be one SELECT predicate that was already used for an index scan, so no
     * point applying it again. A SELECT predicate is represented as elements of
     * this.selectColumnNames, this.selectOperators, and this.selectDataBoxes that
     * correspond to the same index of these lists.
     *
     * @return a new QueryOperator after SELECT has been applied
     */
    private QueryOperator addEligibleSelections(QueryOperator source, int except) {
        for (int i = 0; i < this.selectColumnNames.size(); i++) {
            if (i == except) {
                continue;
            }

            PredicateOperator curPred = this.selectOperators.get(i);
            DataBox curValue = this.selectDataBoxes.get(i);
            try {
                String colName = source.checkSchemaForColumn(source.getOutputSchema(), selectColumnNames.get(i));
                source = new SelectOperator(source, colName, curPred, curValue);
            } catch (QueryPlanException err) {
                /* do nothing */
            }
        }

        return source;
    }

    /**
     * Finds the lowest cost QueryOperator that scans the given table. First
     * determine the cost of a sequential scan for the given table. Then for every index that can be
     * used on that table, determine the cost of an index scan. Keep track of
     * the minimum cost operation. Then push down eligible projects (SELECT
     * predicates). If an index scan was chosen, exclude that SELECT predicate when
     * pushing down selects. This method will be called during the first pass of the search
     * algorithm to determine the most efficient way to access each single table.
     *
     * @return a QueryOperator that has the lowest cost of scanning the given table which is
     * either a SequentialScanOperator or an IndexScanOperator nested within any possible
     * pushed down select operators
     */
    QueryOperator minCostSingleAccess(String table) {

        // Find the cost of a sequential scan of the table
        QueryOperator minOp = new SequentialScanOperator(this.transaction, table);
        int min_cost = minOp.getIOCost(); //init min cost.
        // 2. For each eligible index column, find the cost of an index scan of the
        // table and retain the lowest cost operator
        List<Integer> indCols = getEligibleIndexColumns(table); //eligible index columns.
        int mindex = -1; // Initialize min index.
        for (int col : indCols) {
            IndexScanOperator iScan = getIScanOp(col,table); //get op on column
            int newCost = iScan.getIOCost(); //Cost of scan
            if (newCost < min_cost) {
                //Update minimum cost, cost operator, and index column.
                min_cost = newCost;
                minOp = iScan;
                mindex = col;
            }
        }
        // 3. Push down SELECT predicates that apply to this table and that were not
        // used for an index scan
        minOp = addEligibleSelections(minOp, mindex);
        return minOp;
    }

    /**
     * Given a table and column index, return an index scan operator over this table's column.
     * @param col column index
     * @param table Table name
     * @return IndexScanOperator over column.
     */
    private IndexScanOperator getIScanOp(int col, String table) {
        String colName = selectColumnNames.get(col);
        PredicateOperator pred = selectOperators.get(col);
        DataBox val = selectDataBoxes.get(col);
        IndexScanOperator iScan = new IndexScanOperator(transaction, table, colName, pred, val);
        return iScan;
    }

    /**
     * Given a join condition between an outer relation represented by leftOp
     * and an inner relation represented by rightOp, find the lowest cost join
     * operator out of all the possible join types in JoinOperator.JoinType.
     *
     * @return lowest cost join QueryOperator between the input operators
     */
    private QueryOperator minCostJoinType(QueryOperator leftOp,
                                          QueryOperator rightOp,
                                          String leftColumn,
                                          String rightColumn) {
        QueryOperator minOp = null;

        int minCost = Integer.MAX_VALUE;
        List<QueryOperator> allJoins = new ArrayList<>();
        allJoins.add(new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        allJoins.add(new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));

        for (QueryOperator join : allJoins) {
            int joinCost = join.estimateIOCost();
            if (joinCost < minCost) {
                minOp = join;
                minCost = joinCost;
            }
        }
        return minOp;
    }

    /**
     * Iterate through all table sets in the previous pass of the search. For each
     * table set, check each join predicate to see if there is a valid join
     * condition with a new table. If so, check the cost of each type of join and
     * keep the minimum cost join. Construct and return a mapping of each set of
     * table names being joined to its lowest cost join operator. A join predicate
     * is represented as elements of this.joinTableNames, this.joinLeftColumnNames,
     * and this.joinRightColumnNames that correspond to the same index of these lists.
     *
     * @return a mapping of table names to a join QueryOperator
     */
    Map<Set, QueryOperator> minCostJoins(Map<Set, QueryOperator> prevMap,
                                         Map<Set, QueryOperator> pass1Map) {
        Map<Set, QueryOperator> minTableOps = new HashMap<>();

        //We provide a basic description of the logic you have to implement

        //Input: prevMap (maps a set of tables to a query operator--the operator that joins the set)
        //Input: pass1Map (each set is a singleton with one table and single table access query operator)

        //FOR EACH set of tables in prevMap:
        for (Set tables: prevMap.keySet()) {
            //FOR EACH join condition listed in the query
            for (int i = 0; i < joinTableNames.size(); i++) {
                //get the left side and the right side (table name and column)
                String ltName = getJoinLeftColumnNameByIndex(i)[0];
                String lColName = getJoinLeftColumnNameByIndex(i)[1];
                String rtName = getJoinRightColumnNameByIndex(i)[0];
                String rColName = getJoinRightColumnNameByIndex(i)[1];

                //Containers for min operator and table name to be added to join.
                QueryOperator minOp;
                String tableToMap;
                boolean containsLeft = tables.contains(ltName);
                boolean containsRight = tables.contains(rtName);

                /*
                 * Case 1. Set contains left table but not right, use pass1Map to
                 * fetch the right operator to access the rightTable
                */
                if (containsLeft && !containsRight) {
                    Set<String> rt = new HashSet<>(); //Right table.
                    rt.add(rtName);
                    QueryOperator leftOp = prevMap.get(tables);
                    QueryOperator rightOp = pass1Map.get(rt); //pass1map fetch right operator to get LEFT table
                    minOp = minCostJoinType(leftOp, rightOp, lColName, rColName); //Acquire minimum cost operator.
                    tableToMap = rtName; //SAVE right table name for optimization process.
                }
                /*
                 * Case 2. Set contains right table but not left, use pass1Map to
                 * fetch the right operator to access the leftTable.
                 */
                else if (!containsLeft && containsRight) {
                    Set<String> lt = new HashSet<>(); //Left table
                    lt.add(ltName);
                    QueryOperator leftOp = prevMap.get(tables);
                    QueryOperator rightOp = pass1Map.get(lt);//fetch right operator  ON left table.
                    minOp = minCostJoinType(leftOp, rightOp, rColName, lColName);
                    tableToMap = ltName;
                }
                /*
                 * Case 3. Set contains neither or both the left table or right table (continue loop)
                 */
                else { //Do no mapping.
                    continue;
                }
                //Then given the operator, use minCostJoinType to calculate the cheapest join with that
                //and the previously joined tables.
                Set<String> fullTableSet = new HashSet<>();
                fullTableSet.addAll(tables); //previously joined
                fullTableSet.add(tableToMap); //currently joined
                int min_cost;
                //IF we already got our tables mapped to an op, replace map with cheaper (if exists)
                if (minTableOps.containsKey(fullTableSet)) {
                    QueryOperator min_op = minTableOps.get(fullTableSet);
                    min_cost = min_op.getIOCost();
                    if (min_cost > minOp.getIOCost()) {
                        minTableOps.replace(fullTableSet, minOp);
                    }
                }
                //Otherwise, we map our table to our op.
                else {
                    minTableOps.put(fullTableSet, minOp);
                }
            }

        }
        return minTableOps;
    }

    /**
     * Finds the lowest cost QueryOperator in the given mapping. A mapping is
     * generated on each pass of the search algorithm, and relates a set of tables
     * to the lowest cost QueryOperator accessing those tables. This method is
     * called at the end of the search algorithm after all passes have been
     * processed.
     *
     * @return a QueryOperator in the given mapping
     */
    private QueryOperator minCostOperator(Map<Set, QueryOperator> map) {
        QueryOperator minOp = null;
        QueryOperator newOp;
        int minCost = Integer.MAX_VALUE;
        int newCost;
        for (Set tables : map.keySet()) {
            newOp = map.get(tables);
            newCost = newOp.getIOCost();
            if (newCost < minCost) {
                minOp = newOp;
                minCost = newCost;
            }
        }
        return minOp;
    }

    private String checkIndexEligible() {
        if (this.selectColumnNames.size() > 0
                && this.groupByColumn == null
                && this.joinTableNames.size() == 0) {
            int index = 0;
            for (String column : selectColumnNames) {
                if (this.transaction.indexExists(this.startTableName, column)) {
                    if (this.selectOperators.get(index) != PredicateOperator.NOT_EQUALS) {
                        return column;
                    }
                }

                index++;
            }
        }

        return null;
    }

    private void generateIndexPlan(String indexColumn) {
        int selectIndex = this.selectColumnNames.indexOf(indexColumn);
        PredicateOperator operator = this.selectOperators.get(selectIndex);
        DataBox value = this.selectDataBoxes.get(selectIndex);

        this.finalOperator = new IndexScanOperator(this.transaction, this.startTableName, indexColumn,
                operator,
                value);

        this.selectColumnNames.remove(selectIndex);
        this.selectOperators.remove(selectIndex);
        this.selectDataBoxes.remove(selectIndex);

        this.addSelects();
        this.addProjects();
    }

    private void addJoins() {
        int index = 0;

        for (String joinTable : this.joinTableNames) {
            SequentialScanOperator scanOperator = new SequentialScanOperator(this.transaction, joinTable);

            this.finalOperator = new SNLJOperator(finalOperator, scanOperator,
                                                  this.joinLeftColumnNames.get(index), this.joinRightColumnNames.get(index),
                                                  this.transaction);

            index++;
        }
    }

    private void addSelects() {
        int index = 0;

        for (String selectColumn : this.selectColumnNames) {
            PredicateOperator operator = this.selectOperators.get(index);
            DataBox value = this.selectDataBoxes.get(index);

            this.finalOperator = new SelectOperator(this.finalOperator, selectColumn,
                                                    operator, value);

            index++;
        }
    }

    private void addGroupBy() {
        if (this.groupByColumn != null) {
            if (this.projectColumns.size() > 2 || (this.projectColumns.size() == 1 &&
                                                   !this.projectColumns.get(0).equals(this.groupByColumn))) {
                throw new QueryPlanException("Can only project columns specified in the GROUP BY clause.");
            }

            this.finalOperator = new GroupByOperator(this.finalOperator, this.transaction,
                    this.groupByColumn);
        }
    }

    private void addProjects() {
        if (!this.projectColumns.isEmpty() || this.hasCount || this.sumColumnName != null
                || this.averageColumnName != null) {
            this.finalOperator = new ProjectOperator(this.finalOperator, this.projectColumns,
                    this.hasCount, this.averageColumnName, this.sumColumnName);
        }
    }


    /**
     * Helper function that returns left table name,
     * right table name in 2 elmt arraylist.
     * @return Arraylist containing left table name, right.
     */
    private ArrayList<String> tableNames() {
        return null;
    }
}
