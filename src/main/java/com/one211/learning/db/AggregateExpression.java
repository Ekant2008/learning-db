package com.one211.learning.db;

public interface AggregateExpression extends Expression {
    Object finalValue();

    AggregateExpression fresh();

    public final class Min implements AggregateExpression {
        private final Expression expression;
        Object state = null;
        public Min(Expression expression) {
            this.expression = expression;
        }


        @Override
        public Object finalValue() {
            return state;
        }

        @Override
        public AggregateExpression fresh() {
            return new Min(expression);
        }


        @Override
        public Object apply(Row row) {
            Object current = expression.apply(row);
            if ( state == null) {
                state = current;
            } else {
                state = Math.min((Integer) current, (Integer) state);
            }
            return state;
        }
    }
    public final class Max implements AggregateExpression{
        private final Expression expression;
        Object state = null;
        public Max(Expression expression) {
            this.expression = expression;
        }
        @Override
        public Object finalValue() {
            return state;
        }

        @Override
        public AggregateExpression fresh() {
            return new Max(expression);
        }


        @Override
        public Object apply(Row row)  {
            Object current = expression.apply(row);
            if ( state == null) {
                state = current;
            } else {
                state = Math.max((Integer) current, (Integer) state);
            }
            return state;
        }
    }
    public final class Sum implements AggregateExpression{
        public final Expression expression;
        Double sum =0.0;
        public Sum(Expression expression){
            this.expression=expression;
        }
        @Override
        public Object finalValue() {
            return sum;
        }

        @Override
        public Object apply(Row row) {
            Object current = expression.apply(row);
            if (current instanceof Number) {
                sum += ((Number) current).doubleValue();
            }
            return sum;
        }
        @Override
        public AggregateExpression fresh() {
            return new Sum(expression);
        }

    }
    public final class Count implements AggregateExpression{
        private int count = 0;
        @Override
        public Object finalValue() {
            return count;
        }

        @Override
        public AggregateExpression fresh() {
            return new Count();
        }


        @Override
        public Object apply(Row row) {
            count++;
            return count;
        }


    }
    public final class Average implements AggregateExpression {
        private final Sum sum;
        private final Count count;

        public Average(Expression expression) {
            this.sum = new Sum(expression);
            this.count = new Count();
        }

        @Override
        public Object apply(Row row) {
            sum.apply(row);
            count.apply(row);
            return finalValue();
        }

        @Override
        public Object finalValue() {
            int c = (Integer) count.finalValue();
            if (c == 0) return null;
            double s = (Double) sum.finalValue();
            return s / c;
        }

        @Override
        public AggregateExpression fresh() {
            return new Average(sum.expression); // Reuse same expression
        }

    }

}
