package com.one211.learning.db;


import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.one211.learning.db.Expression.col;
import static org.junit.Assert.assertEquals;

public class TableTest  {

@Test
    public void testFilter() {
    Row row1 = Row.apply("abc", 22);
    Row row2  = Row.apply("xyz", 15);
    Row row3  = Row.apply("zxc", 22);
    var table = new Table.ListBackedTable(List.of(row1, row2,row3));

    var ageExpression = new Expression.BoundedExpression(1);
    var literal = new Expression.Literal(22);
    var age = new Filter.EqualFilter(ageExpression, literal);

    var filteredTable = table.filter(age);

    List<Row> resultRows = new ArrayList<>();
    for (Row row : filteredTable) {
        resultRows.add(row);
    }

    assertEquals(2, resultRows.size());

}
@Test
    public void testProject() {
        Row row1 = Row.apply("abc", 22,"ngp");
        Row row2  = Row.apply("xyz", 15,"cwa");
        Row row3  = Row.apply("zxc", 22,"del");
        var table = new Table.ListBackedTable(List.of(row1, row2,row3));
        var Index0 = new Expression.BoundedExpression(0);

        var Index1 = new Expression.BoundedExpression(1);

        var resultData = table.project(Index0,Index1);
        int count = 0;

        for (Row r: resultData)
        {
            count ++;
        }
        assertEquals(3, count);

        Row row0 = ((Table.ListBackedTable) resultData).rows.get(0);
        assertEquals("abc", row0.get(0));
        assertEquals(22, row0.get(1));

        Row p1 = ((Table.ListBackedTable) resultData).rows.get(1);
        assertEquals("xyz", p1.get(0));
        assertEquals(15, p1.get(1));
    }
@Test
    public void testJoin() {
    var t1 = Row.apply("ash",12);
    var t2 = Row.apply(898978045,"Chhindwara");
        var newTable = t1.join(t2);

        assertEquals(4, newTable.length());
    var t3 = Row.apply("email@gmail.com");
    var newTable2 = newTable.join(t3);
        assertEquals(5, newTable2.length());
    var age = new Expression.BoundedExpression(4);
    assertEquals("email@gmail.com",age.apply(newTable2));


    }



        @Test
        public void testFromJson() throws IOException {
            Table table = Table.AbstractTable.fromJson("Json_data/data.json");

            Iterator<Row> it = table.iterator();
            Row row1 = it.next();
            Row row2 = it.next();

            assertEquals("Alice", row1.get(0));
            assertEquals(30, row1.get(1));
            assertEquals("Bob", row2.get(0));
            assertEquals(25, row2.get(1));
        }
    @Test
    public void testGroupByAndAggregate() {
        List<Row> rows = Arrays.asList(
                Row.apply(new Object[]{"Alice", 10}),
                Row.apply(new Object[]{"Bob", 20}),
                Row.apply(new Object[]{"Alice", 30}),
                Row.apply(new Object[]{"Bob", 25})
        );

        Table table = new Table.ListBackedTable(rows);

        // Group by name (col 0), count and sum col 1
        Table result = table.aggregate(
                col(0),
                new AggregateExpression.Count(),
                new AggregateExpression.Sum(col(1))
        );

        List<Row> results = new ArrayList<>();
        for (Row r : result) {
            results.add(r);
        }

        assertEquals(2, results.size());

        Row aliceRow = results.stream()
                .filter(r -> r.get(0).equals("Alice"))
                .findFirst()
                .orElseThrow();

        assertEquals("Alice", aliceRow.get(0));
        assertEquals(2, aliceRow.get(1));   // Count
        assertEquals(40.0, aliceRow.get(2)); // Sum

        Row bobRow = results.stream()
                .filter(r -> r.get(0).equals("Bob"))
                .findFirst()
                .orElseThrow();

        assertEquals("Bob", bobRow.get(0));
        assertEquals(2, bobRow.get(1));    // Count
        assertEquals(45.0, bobRow.get(2)); // Sum
    }

}