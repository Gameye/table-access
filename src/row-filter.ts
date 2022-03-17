export type RowFilter<Row> =
    AndFilter<Row> | OrFilter<Row> |
    MaximumFilter<Row> | MinimumFilter<Row> |
    EqualFilter<Row>;
export function isRowFilter<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): rowFilter is RowFilter<Row> {
    return "type" in rowFilter && (
        isAndFilter(rowFilter) ||
        isOrFilter(rowFilter) ||
        isMaximumFilter(rowFilter) ||
        isMinimumFilter(rowFilter) ||
        isEqualFilter(rowFilter)
    )
}

export interface EqualFilter<Row, Field extends keyof Row = keyof Row> {
    type: "eq";
    field: Field;
    value: Row[Field];
    invert?: boolean;
}
export function isEqualFilter<Row>(
    rowFilter: RowFilter<Row>
): rowFilter is EqualFilter<Row> {
    return rowFilter.type == "eq" &&
        "field" in rowFilter &&
        "value" in rowFilter;
}
export interface MinimumFilter<Row, Field extends keyof Row = keyof Row> {
    type: "min";
    field: Field;
    value: Row[Field];
    exclusive?: boolean;
}
export function isMinimumFilter<Row>(
    rowFilter: RowFilter<Row>
): rowFilter is MinimumFilter<Row> {
    return rowFilter.type == "min" &&
        "field" in rowFilter &&
        "value" in rowFilter;
}
export interface MaximumFilter<Row, Field extends keyof Row = keyof Row> {
    type: "max";
    field: Field;
    value: Row[Field];
    exclusive?: boolean;
}
export function isMaximumFilter<Row>(
    rowFilter: RowFilter<Row>
): rowFilter is MaximumFilter<Row> {
    return rowFilter.type == "max" &&
        "field" in rowFilter &&
        "value" in rowFilter;
}
export interface AndFilter<Row> {
    type: "and";
    filter: Array<RowFilter<Row> | Partial<Row>>;
}
export function isAndFilter<Row>(
    rowFilter: RowFilter<Row>
): rowFilter is AndFilter<Row> {
    return rowFilter.type == "and" &&
        "filter" in rowFilter;
}
export interface OrFilter<Row> {
    type: "or";
    filter: Array<RowFilter<Row> | Partial<Row>>;
}
export function isOrFilter<Row>(
    rowFilter: RowFilter<Row>
): rowFilter is OrFilter<Row> {
    return rowFilter.type == "or" &&
        "filter" in rowFilter;
}

export function normalizeRowFilter<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): RowFilter<Row> {
    if (isRowFilter(rowFilter)) return rowFilter;

    const filter = Object.entries(rowFilter).map(
        ([field, value]) =>
            ({ type: "eq", field, value } as EqualFilter<Row, keyof Row>),
    );

    return {
        type: "and",
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

    switch (rowFilter.type) {
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
            const { type, filter } = rowFilter;
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
                join(` ${type.toUpperCase()} `);
            const filterSql = subRowFilter.length > 1 ? `(${nakedFilterSql})` : nakedFilterSql;

            return { filterSql, param, paramCount };
        }

        default: throw new Error(`invalid type`);
    }

}

export function makeRowFilterFunction<Row>(
    rowFilter: RowFilter<Row> | Partial<Row>,
): (row: Row) => boolean {
    rowFilter = normalizeRowFilter(rowFilter);

    switch (rowFilter.type) {
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

        default: throw new Error(`invalid type`);
    }
}
