// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Test;

public class WindowTest extends PlanTestBase {

    @Test
    public void testLagWindowFunction() throws Exception {
        String sql = "select lag(id_datetime, 1, '2020-01-01') over(partition by t1c) from test_all_type;";
        String plan = getThriftPlan(sql);
        assertContains(plan, "signature:lag(DATETIME, BIGINT, DATETIME)");

        sql = "select lag(id_decimal, 1, 10000) over(partition by t1c) from test_all_type;";
        plan = getThriftPlan(sql);
        String expectSlice = "fn:TFunction(name:TFunctionName(function_name:lag), binary_type:BUILTIN," +
                " arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL64," +
                " precision:10, scale:2))])], ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))]), has_var_args:false, " +
                "signature:lag(DECIMAL64(10,2))";
        Assert.assertTrue(plan.contains(expectSlice));

        sql = "select lag(null, 1,1) OVER () from t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(NULL, 1, 1), ]");

        sql = "select lag(id_datetime, 1, '2020-01-01xxx') over(partition by t1c) from test_all_type;";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("The third parameter of `lag` can't not convert to DATETIME");
        getThriftPlan(sql);
    }

    @Test
    public void testPruneWindowColumn() throws Exception {
        String sql = "select sum(t1c) from (select t1c, lag(id_datetime, 1, '2020-01-01') over( partition by t1c)" +
                "from test_all_type) a ;";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "ANALYTIC");
    }

    @Test
    public void testWindowFunctionTest() throws Exception {
        String sql = "select sum(id_decimal - ifnull(id_decimal, 0)) over (partition by t1c) from test_all_type";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(
                plan.contains("decimal_literal:TDecimalLiteral(value:0, integer_value:00 00 00 00 00 00 00 00)"));
    }

    @Test
    public void testSameWindowFunctionReuse() throws Exception {
        String sql = "select sum(v1) over() as c1, sum(v1) over() as c2 from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 4> : 4: sum(1: v1)\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select sum(v1) over(order by v2 rows between 1 preceding and 1 following) as sum_v1_1," +
                " sum(v1) over(order by v2 rows between 1 preceding and 1 following) as sum_v1_2 from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 4> : 4: sum(1: v1)\n" +
                "  |  \n" +
                "  3:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v1) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 5> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ]");

        sql = "select c1+1, c2+2 from (select sum(v1) over() as c1, sum(v3) over() as c2 from t0) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  <slot 6> : 4: sum(1: v1) + 1\n" +
                "  |  <slot 7> : 5: sum(3: v3) + 2\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(1: v1), ], [, sum(3: v3), ]");
    }

    @Test
    public void testLeadAndLagFunction() {
        String sql = "select LAG(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lag` can't not convert");

        sql = "select lead(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lead` can't not convert");

        sql = "select lead(k3, 3, 'kks') OVER () from baseall";
        starRocksAssert.query(sql)
                .analysisError("Convert type error in offset fn(default value); old_type=VARCHAR new_type=INT");

        sql = "select lead(id2, 1, 1) OVER () from bitmap_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lead(bitmap,");

        sql = "select lag(id2, 1, 1) OVER () from hll_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lag(hll,");
    }

    @Test
    public void testWindowWithAgg() throws Exception {
        String sql = "SELECT v1, sum(v2),  sum(v2) over (ORDER BY v1) AS `rank` FROM t0 group BY v1, v2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");

        sql =
                "SELECT v1, sum(v2),  sum(v2) over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank`  FROM t0 group BY v1, v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
    }

    @Test
    public void testWindowWithChildProjectAgg() throws Exception {
        String sql = "SELECT v1, sum(v2) as x1, row_number() over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank` " +
                "FROM t0 group BY v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 8> : if(CAST(1: v1 AS BOOLEAN), 1, NULL)");
    }

    @Test
    public void testWindowPartitionAndSortSameColumn() throws Exception {
        String sql = "SELECT k3, avg(k3) OVER (partition by k3 order by k3) AS sum FROM baseall;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, avg(3: k3), ]\n" +
                "  |  partition by: 3: k3\n" +
                "  |  order by: 3: k3 ASC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: <slot 3> 3: k3 ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testWindowDuplicatePartition() throws Exception {
        String sql = "select max(v3) over (partition by v2,v2,v2 order by v2,v2) from t0;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0");

    }

    @Test
    public void testWindowDuplicatedColumnInPartitionExprAndOrderByExpr() throws Exception {
        String sql = "select v1, sum(v2) over (partition by v1, v2 order by v2 desc) as sum1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertNotNull(plan);
    }

    @Test
    public void testSupersetEnforce() throws Exception {
        String sql = "select * from (select v3, rank() over (partition by v1 order by v2) as j1 from t0) as x0 "
                + "join t1 on x0.v3 = t1.v4 order by x0.v3, t1.v4 limit 100;";
        getFragmentPlan(sql);
    }

    @Test
    public void testNtileWindowFunction() throws Exception {
        // Must have exactly one positive bigint integer parameter.
        String sql = "select v1, v2, NTILE(v2) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(1.1) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(0) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(-1) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE('abc') over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(null) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(1 + 2) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql = "select v1, v2, NTILE(9223372036854775808) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql).analysisError("Number out of range");

        sql = "select v1, v2, NTILE((select v1 from t0)) over (partition by v1 order by v2) as j1 from t0";
        starRocksAssert.query(sql)
                .analysisError("The num_buckets parameter of NTILE must be a constant positive integer");

        sql =
                "select v1, v2, NTILE() over (partition by v1 order by v2 rows between 1 preceding and 1 following) as j1 from t0";
        starRocksAssert.query(sql).analysisError("No matching function with signature: ntile()");

        // Windowing clause not allowed with NTILE.
        sql =
                "select v1, v2, NTILE(2) over (partition by v1 order by v2 rows between 1 preceding and 1 following) as j1 from t0";
        starRocksAssert.query(sql).analysisError("Windowing clause not allowed");

        // Normal case.
        sql = "select v1, v2, NTILE(2) over (partition by v1 order by v2) as j1 from t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, ntile(2), ]\n" +
                "  |  partition by: 1: v1\n" +
                "  |  order by: 2: v2 ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT RO");
    }

    @Test
    public void testRankWithoutPartitionPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:SELECT\n" +
                    "  |  predicates: 4: row_number() <= 4\n" +
                    "  |  \n" +
                    "  3:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  2:MERGING-EXCHANGE\n" +
                    "     limit: 4");
        }
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (order by v2) as rk, " +
                    "        sum(v1) over (order by v2) as sm " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:SELECT\n" +
                    "  |  predicates: 4: row_number() <= 4\n" +
                    "  |  \n" +
                    "  4:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  3:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  2:MERGING-EXCHANGE");
        }
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v2) as sm, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  5:SELECT\n" +
                    "  |  predicates: 5: row_number() <= 4\n" +
                    "  |  \n" +
                    "  4:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  3:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  2:MERGING-EXCHANGE");
        }
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        sum(v1) over (order by v3) as sm, " +
                    "        row_number() over (order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  6:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  5:MERGING-EXCHANGE\n" +
                    "     limit: 4");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRankWithPartitionPredicatePushDown() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v3 order by v2) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            System.out.println(sql);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:ANALYTIC\n" +
                    "  |  functions: [, row_number(), ]\n" +
                    "  |  partition by: 3: v3\n" +
                    "  |  order by: 2: v2 ASC\n" +
                    "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  3:SORT\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  2:EXCHANGE");

            assertContains(plan, "  1:PARTITION-TOP-N\n" +
                    "  |  partition by: 3: v3 \n" +
                    "  |  partition limit: 4\n" +
                    "  |  order by: <slot 3> 3: v3 ASC, <slot 2> 2: v2 ASC\n" +
                    "  |  offset: 0");
        }
        {
            // TODO(hcf) Do not support multi partition by right now, this test need to be updated when supported
            String sql = "select * from (\n" +
                    "    select *, " +
                    "        row_number() over (partition by v2, v3 order by v1) as rk " +
                    "    from t0\n" +
                    ") sub_t0\n" +
                    "where rk <= 4;";
            System.out.println(sql);
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SORT\n" +
                    "  |  order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC, <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  1:EXCHANGE");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testRuntimeFilterPushWithoutPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, sum(v2) over (order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";

        String plan = getVerboseExplain(sql);
        assertContains(plan, "3:ANALYTIC\n" +
                "  |  functions: [, sum[([2: v2, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], ]\n" +
                "  |  order by: [2: v2, BIGINT, true] DESC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  cardinality: 1\n" +
                "  |  probe runtime filters:\n" +
                "  |  - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testRuntimeFilterPushWithRightPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, sum(v2) over (partition by v1 order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:SORT\n" +
                "  |  order by: [1, BIGINT, true] ASC, [2, BIGINT, true] DESC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  probe runtime filters:\n" +
                "  |  - filter_id = 0, probe_expr = (1: v1)");
    }

    @Test
    public void testRuntimeFilterPushWithOtherPartition() throws Exception {
        String sql = "select * from " +
                "(select v1, v3, sum(v2) over (partition by v3 order by v2 desc) as sum1 from t0) a," +
                "(select v1 from t0 where v1 = 1) b " +
                "where a.v1 = b.v1";

        String plan = getVerboseExplain(sql);
<<<<<<< HEAD
        assertContains(plan, "3:ANALYTIC\n" +
                "  |  functions: [, sum[([2: v2, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], ]\n" +
=======
        assertContains(plan, "  2:ANALYTIC\n" +
                "  |  functions: [, sum[([2: v2, BIGINT, true]); args: BIGINT; " +
                "result: BIGINT; args nullable: true; result nullable: true], ]\n" +
>>>>>>> 45f45d98e ([BugFix] Fix wrong state of 'isExecuteInOneTablet' when it comes to agg or analytic (#19690))
                "  |  partition by: [3: v3, BIGINT, true]\n" +
                "  |  order by: [2: v2, BIGINT, true] DESC\n" +
                "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  cardinality: 1\n" +
                "  |  probe runtime filters:\n" +
                "  |  - filter_id = 0, probe_expr = (1: v1)");
    }
<<<<<<< HEAD
=======

    @Test
    public void testHashBasedWindowTest() throws Exception {
        {
            String sql = "select sum(v1) over ([hash] partition by v1,v2 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ], [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
        {
            String sql = "select sum(v1) over ( partition by v1,v2 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  4:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  partition by: 1: v1, 2: v2");
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
        {
            String sql = "select sum(v1) over ([hash] partition by v1 )," +
                    "sum(v1/v3) over ([hash] partition by v1,v2 ) " +
                    "from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  3:ANALYTIC\n" +
                    "  |  functions: [, sum(1: v1), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  useHashBasedPartition");
            assertContains(plan, "  1:ANALYTIC\n" +
                    "  |  functions: [, sum(CAST(1: v1 AS DOUBLE) / CAST(3: v3 AS DOUBLE)), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  useHashBasedPartition");
        }
    }

    @Test
    public void testOneTablet() throws Exception {
        {
            String sql = "select v1 from (" +
                    "select v1, dense_rank() over (partition by v1 order by v3) rk from t0" +
                    ") temp " +
                    "where rk = 1 " +
                    "group by v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, dense_rank(), ]\n" +
                    "  |  partition by: 1: v1\n" +
                    "  |  order by: 3: v3 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  1:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");
            assertContains(plan, "  5:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  4:Project\n" +
                    "  |  <slot 1> : 1: v1");
        }
        {
            String sql = "select v1 from (" +
                    "select v1, dense_rank() over (partition by v1, v2 order by v3) rk from t0" +
                    ") temp " +
                    "where rk = 1 " +
                    "group by v1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:ANALYTIC\n" +
                    "  |  functions: [, dense_rank(), ]\n" +
                    "  |  partition by: 1: v1, 2: v2\n" +
                    "  |  order by: 3: v3 ASC\n" +
                    "  |  window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                    "  |  \n" +
                    "  1:SORT\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 2> 2: v2 ASC, <slot 3> 3: v3 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0");
            assertContains(plan, "  7:AGGREGATE (merge finalize)\n" +
                    "  |  group by: 1: v1\n" +
                    "  |  \n" +
                    "  6:EXCHANGE");
        }
    }
>>>>>>> 45f45d98e ([BugFix] Fix wrong state of 'isExecuteInOneTablet' when it comes to agg or analytic (#19690))
}
