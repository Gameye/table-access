import pg from "pg";
import { UnexpectedRowCountError } from "./error.js";
import { RowDescriptor } from "./row-descriptor.js";
import { makeRowFilterPg, RowFilter } from "./row-filter.js";

/**
 * Retrieves exactly one row from the specified table, if no or more than
 * one row matches the provided filter, an exception is thrown.
 * @param descriptor described the table to retrieve data from
 * @param filter a filter that will return exactly one row
 */
export async function querySingle<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: RowFilter<Row> | Partial<Row>,
): Promise<Row> {
    const { schema, table } = descriptor;
    const rows = await queryMultiple(client, descriptor, filter);

    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [record] = rows;
    return record;
}

/**
 * Returns null or one row from the provided table. If the filter matches
 * more than one row, an exceptoin is thrown
 * @param descriptor describes the table to retrieve data from
 * @param filter a filter to match the row againts
 */
export async function querySingleOrNull<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: RowFilter<Row> | Partial<Row>,
): Promise<Row | null> {
    const { schema, table } = descriptor;
    const rows = await queryMultiple(client, descriptor, filter);

    if (rows.length < 1) return null;
    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [record] = rows;
    return record;
}

/**
 * returns many rows from the provided table, mathing them to the provided
 * filter. If no match is found this function will return an empty array
 * @param descriptor the table to retrieve data from
 * @param filter a filter to match to the rows
 */
export async function queryMultiple<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: RowFilter<Row> | Partial<Row>,
): Promise<Row[]> {
    const { schema, table } = descriptor;
    const filterResult = makeRowFilterPg(filter, "r");

    const result = await client.query(`
SELECT row_to_json(r) AS o
FROM "${schema}"."${table}" AS r
${filterResult.paramCount ? `WHERE ${filterResult.filterSql}` : ""}
;`, filterResult.param);

    const { rows } = result;

    return rows.map(row => row.o);
}

/**
 * insert a row in the provided table and return the inserted row
 * @param descriptor the table to insert a row into
 * @param row The row to insert
 */
export async function queryInsert<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    row: Partial<Row>,
): Promise<Row> {
    const { schema, table } = descriptor;

    const itemFields = Object.keys(row) as Array<keyof Row>;
    const itemValues = itemFields.map(f => row[f]);

    const result = await client.query(`
WITH r AS (
    INSERT INTO "${schema}"."${table}" (${itemFields.map(f => `"${f}"`).join(",")})
    VALUES (${itemFields.map((f, i) => `$${i + 1}`).join(",")})
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, itemValues);

    const { rows } = result;
    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [resultingRow] = rows;

    return resultingRow.o;
}

/**
 * Update exactly one row in the provided table. If the filter matches zero
 * or more than one row, an exception is thrown
 * @param descriptor the table to update a row in
 * @param filter  a filter that will math exactly one row to update
 * @param row the new, updated row
 */
export async function queryUpdate<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: Partial<Row>,
    row: Partial<Row>,
): Promise<Row> {
    const { schema, table } = descriptor;

    const filterFields = Object.keys(filter) as Array<keyof Row>;
    const filterValues = filterFields.map(f => filter[f]);

    const itemFields = Object.keys(row) as Array<keyof Row>;
    const itemValues = itemFields.map(f => row[f]);

    const result = await client.query(`
WITH r AS (
    UPDATE "${schema}"."${table}"
    SET ${itemFields.map((f, i) => `"${f}"=$${i + 1 + filterFields.length}`).join(",")}
    WHERE ${filterFields.map((f, i) => `"${f}"=$${i + 1}`).join(" AND ")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, [...filterValues, ...itemValues]);

    const { rows } = result;
    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [resultingRow] = rows;

    return resultingRow.o;
}

/**
 * Update or insert (upsert!) a row that matches the provided filter in the
 * provided table. If no row is found to update, an insert is done! If
 * there are more than one rows found, an exception it thrown.
 * @param descriptor The table to perform the upsert on
 * @param filter a filter that will be used to find the row to update
 * @param row The new row
 */
export async function queryUpsert<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: Partial<Row>,
    row: Partial<Row>,
): Promise<Row> {
    const { schema, table } = descriptor;

    const filterFields = Object.keys(filter) as Array<keyof Row>;
    const filterValues = filterFields.map(f => filter[f]);

    const rowFields = Object.keys(row) as Array<keyof Row>;
    const rowValues = rowFields.map(f => row[f]);

    const result = await client.query(`
WITH r AS (
    INSERT INTO "${schema}"."${table}" (
        ${[...filterFields, ...rowFields].map(f => `"${f}"`).join(",")}
    )
    VALUES (
        ${[...filterFields, ...rowFields].map((f, i) => `$${i + 1}`).join(",")}
    )
    ON CONFLICT (${filterFields.map(f => `"${f}"`).join(",")}) DO UPDATE
    SET ${rowFields.map((f, i) => `"${f}"=EXCLUDED.${f}`).join(",")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, [...filterValues, ...rowValues]);

    const { rows } = result;
    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [resultingRow] = rows;

    return resultingRow.o;
}

/**
 * Will delete exactly one row from the provided table. If more than one
 * row is deleted or if no row will be deleted, this function will throw an
 * exception.
 * @param descriptor the table to delete the row from
 * @param filter a filter to match the row aginast
 */
export async function queryDelete<Row extends object>(
    client: pg.ClientBase,
    descriptor: RowDescriptor<Row>,
    filter: Partial<Row>,
): Promise<Row> {
    const { schema, table } = descriptor;

    const filterFields = Object.keys(filter) as Array<keyof Row>;
    const filterValues = filterFields.map(f => filter[f]);

    const result = await client.query(`
WITH r AS (
    DELETE FROM "${schema}"."${table}"
    WHERE ${filterFields.map((f, i) => `"${f}"=$${i + 1}`).join(" AND ")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, filterValues);

    const { rows } = result;
    if (rows.length !== 1) throw new UnexpectedRowCountError(
        schema,
        table,
        1,
        rows.length,
    );

    const [resultingRow] = rows;

    return resultingRow.o;
}
