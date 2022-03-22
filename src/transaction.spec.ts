import assert from "assert";
import pg from "pg";
import test from "tape-promise/tape.js";
import { UnexpectedRowCountError } from "./error.js";
import * as q from "./queries.js";
import { RowDescriptor } from "./row-descriptor.js";
import { withTestContext } from "./test-context.js";
import { withTransaction } from "./transaction.js";

async function initializeMocks(pool: pg.Pool) {
    const sql = `
    CREATE TABLE public.one(
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE
    );
    INSERT INTO public.one(name)
    VALUES('one'), ('two');
    `;

    await pool.query(sql);
}

interface OneRow {
    id: number;
    name: string;
}

const OneRowDescriptor: RowDescriptor<OneRow> = {
    schema: "public",
    table: "one",
};

test(
    "TableTransaction#single",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        {
            const row = await withTransaction(pool, client => q.querySingle(
                client,
                OneRowDescriptor,
                { id: 2 },
            ));

            t.deepEqual(row, { id: 2, name: "two" });
        }
        try {
            const row = await withTransaction(pool, client => q.querySingle(
                client,
                OneRowDescriptor,
                { id: 4 },
            ));

            t.fail();
        }
        catch (error) {
            t.ok(error instanceof UnexpectedRowCountError);
        }
    }),
);

test(
    "TableTransaction#singleOrNull",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        {
            const row = await withTransaction(pool, client => q.querySingleOrNull(
                client,
                OneRowDescriptor,
                { id: 2 },
            ));

            t.deepEqual(row, { id: 2, name: "two" });
        }

        {
            const row = await withTransaction(pool, client => q.querySingleOrNull(
                client,
                OneRowDescriptor,
                { id: 4 },
            ));

            t.equal(row, null);
        }
    }),

);

test(
    "TableTransaction#multiple",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        const rows = await withTransaction(pool, client => q.queryMultiple(
            client,
            OneRowDescriptor,
            { id: 2 },
        ));

        t.deepEqual(rows, [{ id: 2, name: "two" }]);
    }),
);

test(
    "TableTransaction#insert",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        {
            const row = await withTransaction(pool, client => q.queryInsert(
                client,
                OneRowDescriptor,
                { name: "three" },
            ));

            t.deepEqual(row, { id: 3, name: "three" });
        }

        try {
            const row = await withTransaction(pool, client => q.queryInsert(
                client,
                OneRowDescriptor,
                { id: 1, name: "four" },
            ));

            t.fail();
        }
        catch (error) {
            assert(error instanceof pg.DatabaseError);
            t.equal(error.code, "23505");
        }

        try {
            const row = await withTransaction(pool, client => q.queryInsert(
                client,
                OneRowDescriptor,
                { id: 5, name: "one" },
            ));

            t.fail();
        }
        catch (error) {
            assert(error instanceof pg.DatabaseError);
            t.equal(error.code, "23505");
        }
    }),
);

test(
    "TableTransaction#update",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        {
            const row = await withTransaction(pool, client => q.queryUpdate(
                client,
                OneRowDescriptor,
                { name: "one" },
                { name: "een" },
            ));

            t.deepEqual(row, { id: 1, name: "een" });
        }

        try {
            const row = await withTransaction(pool, client => q.queryUpdate(
                client,
                OneRowDescriptor,
                { name: "one" },
                { name: "een" },
            ));

            t.fail();
        }
        catch (error) {
            t.ok(error instanceof UnexpectedRowCountError);
        }
    }),
);

test(
    "TableTransaction#upsert",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        const row = await withTransaction(pool, client => q.queryUpsert(
            client,
            OneRowDescriptor,
            { id: 2 },
            { name: "twee" },
        ));

        t.deepEqual(row, { id: 2, name: "twee" });
    }),
);

test(
    "TableTransaction#delete",
    async t => withTestContext(async ({ pool }) => {
        await initializeMocks(pool);

        const row = await withTransaction(pool, client => q.queryDelete(
            client,
            OneRowDescriptor,
            { id: 2 },
        ));

        t.deepEqual(row, { id: 2, name: "two" });
    }),
);
