package com.one211.learning.db;



import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class AggregateExpressionTest {

    private static final Expression intColumnExpr = row -> (Integer) row.get(0);


    private Row row1;
    private Row row2;
    private Row row3;
    private Row row4;

    @Before
    public void setup() {

        row1 = Row.apply(5);
        row2 = Row.apply(3);
        row3 = Row.apply(8);
        row4 = Row.apply(10);
    }

    @Test
    public void testMin() {
        AggregateExpression.Min min = new AggregateExpression.Min(intColumnExpr);
        List<Row> rows = List.of(row1, row2, row3);
        for (Row r : rows) min.apply(r);
        assertEquals(3, min.finalValue());
    }

    @Test
    public void testMax() {
        AggregateExpression.Max max = new AggregateExpression.Max(intColumnExpr);
        List<Row> rows = List.of(row1, row2, row3);
        for (Row r : rows) max.apply(r);
        assertEquals(8, max.finalValue());
    }

    @Test
    public void testSum() {
        AggregateExpression.Sum sum = new AggregateExpression.Sum(intColumnExpr);
        List<Row> rows = List.of(row1, row2, row4);
        for (Row r : rows) sum.apply(r);
        assertEquals(18.0, sum.finalValue());
    }

    @Test
    public void testCount() {
        AggregateExpression.Count count = new AggregateExpression.Count();
        List<Row> rows = List.of(row1, row2, row3, row4);
        for (Row r : rows) count.apply(r);
        assertEquals(4, count.finalValue());
    }

    @Test
    public void testAverage() {
        AggregateExpression.Average avg = new AggregateExpression.Average(intColumnExpr);
        List<Row> rows = List.of(row1, row2, row4);
        for (Row r : rows) avg.apply(r);
        assertEquals(6.0, avg.finalValue());
    }
}
