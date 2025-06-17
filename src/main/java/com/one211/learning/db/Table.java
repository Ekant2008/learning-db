package com.one211.learning.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.swing.plaf.ListUI;
import java.io.File;
import java.io.IOException;
import java.util.*;

public interface Table extends Iterable<Row> {
    Table filter(Filter filter);
    Table project(Expression... projections);
    Table join(Table input);

    Table aggregate(Expression groupBy, AggregateExpression... expression);

    abstract class AbstractTable implements Table {
        public Table filter(Filter filter) {
            List<Row> filtered = new ArrayList<>();
            Iterator<Row> it = this.iterator();

            while (it.hasNext()) {
                Row row = it.next();
                if ((Boolean) filter.apply(row)) {
                    filtered.add(row);
                }
            }

            return new ListBackedTable(filtered);
        }

        public Table project(Expression... projections) {
            List<Row> projected = new ArrayList<>();  // Initialize the list
            Iterator<Row> it = this.iterator();

            while (it.hasNext()) {
                Row row = it.next();
                Object[] projectedVal = new Object[projections.length];
                for (int i = 0; i < projections.length; i++) {
                    projectedVal[i] = projections[i].apply(row);
                }
                projected.add(Row.apply(projectedVal));
            }

            return new ListBackedTable(projected);
        }


        public Table join(Table input) {
            List<Row> result = new ArrayList<>();
            Iterator<Row> outer = this.iterator();

            while (outer.hasNext()) {
                Row thisRow = outer.next();
                Iterator<Row> inner = input.iterator();
                while (inner.hasNext()) {
                    Row thatRow = inner.next();
                    result.add(thisRow.join(thatRow));
                }
            }
            return new ListBackedTable(result);
        }



        public static Table fromJson(String filePath) throws IOException {
            ObjectMapper mapper = new ObjectMapper();

                List<Map<String, Object>> jsonData = mapper.readValue(
                        new File(filePath),
                        new TypeReference<>() {}
                );

                List<Row> rows = new ArrayList<>();
                for (Map<String, Object> record : jsonData) {
                    Object[] values = record.values().toArray();
                    rows.add(Row.apply(values));
                }

                return new ListBackedTable(rows);

        }

        @Override
        public Table aggregate(Expression groupBy, AggregateExpression... expression) {
            Map<Object, List<Row>> grouped = new HashMap<Object, List<Row>>();

            Iterator<Row> thisIterator = this.iterator();

            while (thisIterator.hasNext()){
                Row currentRow = thisIterator.next();
                Object key = groupBy.apply(currentRow);
                grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(currentRow);
            }

            List<Row> resultRows = new ArrayList<>();

            for (Map.Entry<Object, List<Row>> entry : grouped.entrySet()) {
                Object groupKey = entry.getKey();
                List<Row> rowsInGroup = entry.getValue();

                // Create fresh instances of each aggregation per group
                AggregateExpression[] aggInstances = new AggregateExpression[expression.length];
                for (int i = 0; i < expression.length; i++) {
                    aggInstances[i] = expression[i].fresh();
                }

                // Apply rows to aggregate expressions
                for (Row r : rowsInGroup) {
                    for (AggregateExpression agg : aggInstances) {
                        agg.apply(r);
                    }
                }

                // Collect results: group key + all aggregate final values
                Object[] result = new Object[aggInstances.length + 1];
                result[0] = groupKey;
                for (int i = 0; i < aggInstances.length; i++) {
                    result[i + 1] = aggInstances[i].finalValue();
                }

                resultRows.add(Row.apply(result));
            }

            return new ListBackedTable(resultRows);
        }

    }





    class ListBackedTable extends AbstractTable {

        List<Row> rows;
        public ListBackedTable(List<Row> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<Row> iterator() {
            return rows.stream().iterator();
        }

    }
}

