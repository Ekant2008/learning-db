package com.one211.learning.db;

public interface AggregateExpression extends Expression {
    Object finalValue();

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
}
