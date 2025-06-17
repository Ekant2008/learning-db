package com.one211.learning.db;

public interface Expression {
    Object apply(Row row);

    record BinaryArithmetic(Expression left, Expression right, String operator) implements Expression {
        @Override
        public Object apply(Row row) {
            Object leftVal = left.apply(row);
            Object rightVal = right.apply(row);
            if (leftVal instanceof Number && rightVal instanceof Number) {
                Integer l = (Integer) leftVal;
                Integer r = (Integer) rightVal;
                return switch (operator) {
                    case "+" -> l + r;
                    case "-" -> l - r;
                    case "*" -> l * r;
                    case "/" -> r != 0 ? l / r : null;
                    default -> throw new IllegalArgumentException("Unknown operator: " + operator);
                };
            }
            throw new IllegalArgumentException("Operands must be numbers: " + leftVal + "And One is: " + rightVal);
        }
    }

    record Literal(Object literal) implements Expression {
        @Override
        public Object apply(Row row) {
            return literal;
        }
    }

    record BoundedExpression(int index ) implements Expression {
        @Override
        public Object apply(Row row) {
            return row.get(index);
        }
    }
    static BoundedExpression col(int index) {
        return new BoundedExpression(index);
    }

}