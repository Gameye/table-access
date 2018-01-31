export type RowFilter<Row> =
    AndFilter<Row> | OrFilter<Row> |
    MaximumFilter<Row, keyof Row> | MinimumFilter<Row, keyof Row> |
    EqualFilter<Row, keyof Row>;

export interface EqualFilter<Row, Field extends keyof Row> {
    _ft: "eq";
    field: Field;
    value: Row[Field];
    invert?: boolean;
}
export interface MinimumFilter<Row, Field extends keyof Row> {
    _ft: "min";
    field: Field;
    value: Row[Field];
    exclusive?: boolean;
}
export interface MaximumFilter<Row, Field extends keyof Row> {
    _ft: "max";
    field: Field;
    value: Row[Field];
    exclusive?: boolean;
}
export interface AndFilter<Row> {
    _ft: "and";
    filter: Array<RowFilter<Row> | Partial<Row>>;
}
export interface OrFilter<Row> {
    _ft: "or";
    filter: Array<RowFilter<Row> | Partial<Row>>;
}

export function isRowFilter<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): rowFilter is RowFilter<Row> {
    return "_ft" in rowFilter;
}

export function normalizeRowFilter<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): RowFilter<Row> {
    if (isRowFilter(rowFilter)) return rowFilter;

    const filter = Object.entries(rowFilter).map(
        ([field, value]) =>
            ({ _ft: "eq", field, value } as EqualFilter<Row, keyof Row>),
    );

    return {
        _ft: "and",
        filter,
    };
}

export interface SqlFilterResult {
    filterSql: string;
    param: any[];
    paramCount: number;
}

export function makeRowFilterPg<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
    tableName: string,
    paramOffset: number = 0,
): SqlFilterResult {
    rowFilter = normalizeRowFilter(rowFilter);

    switch (rowFilter._ft) {
        case "eq": {
            const { field, value, invert } = rowFilter;
            if (value === null) {
                if (invert) return {
                    filterSql: `"${tableName}"."${field}" IS NOT NULL`,
                    param: [],
                    paramCount: 0,
                };
                else return {
                    filterSql: `"${tableName}"."${field}" IS NULL`,
                    param: [],
                    paramCount: 0,
                };
            }
            else {
                if (invert) return {
                    filterSql: `"${tableName}"."${field}" <> $${paramOffset + 1}`,
                    param: [value],
                    paramCount: 1,
                };
                else return {
                    filterSql: `"${tableName}"."${field}" = $${paramOffset + 1}`,
                    param: [value],
                    paramCount: 1,
                };
            }
        }

        case "min": {
            const { field, value, exclusive } = rowFilter;
            if (exclusive) return {
                filterSql: `"${tableName}"."${field}" > $${paramOffset + 1}`,
                param: [value],
                paramCount: 1,
            };
            else return {
                filterSql: `"${tableName}"."${field}" >= $${paramOffset + 1}`,
                param: [value],
                paramCount: 1,
            };
        }

        case "max": {
            const { field, value, exclusive } = rowFilter;
            if (exclusive) return {
                filterSql: `"${tableName}"."${field}" < $${paramOffset + 1}`,
                param: [value],
                paramCount: 1,
            };
            else return {
                filterSql: `"${tableName}"."${field}" <= $${paramOffset + 1}`,
                param: [value],
                paramCount: 1,
            };
        }

        case "or":
        case "and": {
            const { _ft, filter } = rowFilter;
            let paramCount = 0;
            const subRowFilter = filter.map(
                f => {
                    const filterResult = makeRowFilterPg(f, tableName, paramOffset + paramCount);
                    paramCount += filterResult.paramCount;
                    return filterResult;
                },
            );

            const param = ([] as SqlFilterResult[]).concat(
                ...subRowFilter.map(f => f.param),
            );

            const nakedFilterSql = subRowFilter.
                map(f => f.filterSql).
                join(` ${_ft.toUpperCase()} `);
            const filterSql = subRowFilter.length > 1 ? `(${nakedFilterSql})` : nakedFilterSql;

            return { filterSql, param, paramCount };
        }

        default: throw new Error(`invalid _ft`);
    }

}

export function makeRowFilterFunction<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): (row: Row) => boolean {
    rowFilter = normalizeRowFilter(rowFilter);

    switch (rowFilter._ft) {
        case "eq": {
            const { field, value, invert } = rowFilter;
            if (invert) return (row: Row) => row[field] !== value;
            else return (row: Row) => row[field] === value;
        }

        case "min": {
            const { field, value, exclusive } = rowFilter;
            if (exclusive) return (row: Row) => row[field] !== null && row[field] > value;
            else return (row: Row) => row[field] !== null && row[field] >= value;
        }

        case "max": {
            const { field, value, exclusive } = rowFilter;
            if (exclusive) return (row: Row) => row[field] !== null && row[field] < value;
            else return (row: Row) => row[field] !== null && row[field] <= value;
        }

        case "or": {
            const { filter } = rowFilter;
            const fns = filter.map(makeRowFilterFunction);
            return (row: Row) => fns.some(fn => fn(row));
        }

        case "and": {
            const { filter } = rowFilter;
            const fns = filter.map(makeRowFilterFunction);
            return (row: Row) => fns.every(fn => fn(row));
        }

        default: throw new Error(`invalid _ft`);
    }
}
