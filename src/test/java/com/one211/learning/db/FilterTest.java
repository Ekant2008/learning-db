package com.one211.learning.db;




import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTest {


    @Test
    public void notNullTest() {
        var testRow = Row.apply("one", null, "two");
        var notNull0 = new Filter.NotNullFilter(new Expression.BoundedExpression(0));
        assertTrue((Boolean) notNull0.apply(testRow));

        var notNull1 = new Filter.NotNullFilter(new Expression.BoundedExpression(1));
        assertFalse((Boolean) notNull1.apply(testRow));
    }

    @Test
    public void andTest() {
        var row = Row.apply(true, true);
        var left = new Expression.BoundedExpression(0);
        var right = new Expression.BoundedExpression(1);
        var andFilter = new Filter.AndFilter(left, right);
        assertTrue(andFilter.apply(row));
    }

    @Test
    public void orTest() {
        var row = Row.apply(false, true);
        var left = new Expression.BoundedExpression(0);
        var right = new Expression.BoundedExpression(1);
        var orFilter = new Filter.OrFilter(left, right);
        assertTrue(orFilter.apply(row));
    }

    @Test
    public void isEqualTest() {
        var testRow = Row.apply("Ekant", "Ekant");
        var isEqualTest = new Filter.EqualFilter(
                new Expression.BoundedExpression(0),
                new Expression.BoundedExpression(1)
        );
        assertTrue(isEqualTest.apply(testRow));
    }


    }



