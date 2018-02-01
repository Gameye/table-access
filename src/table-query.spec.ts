import * as test from "blue-tape";
import { NotFound } from "http-errors";
import { using } from "using-disposable";
import { DatabaseTestContext } from "./database-test-context";
import { TableDescriptor } from "./table-descriptor";
import { TableQuery } from "./table-query";

const sql = `
CREATE TABLE public.one(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
INSERT INTO public.one(name)
VALUES('one'), ('two');
`;

interface OneTableRow {
    id: number;
    name: string;
}

const OneTableDescriptor: TableDescriptor<OneTableRow> = {
    schema: "public",
    table: "one",
};

test(
    "TableQuery#single",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        {
            const row = await TableQuery.query(pool, q => q.single(
                OneTableDescriptor,
                { id: 2 },
            ));

            t.deepEqual(row, { id: 2, name: "two" });
        }
        try {
            const row = await TableQuery.query(pool, q => q.single(
                OneTableDescriptor,
                { id: 4 },
            ));

            t.fail();
        }
        catch (e) {
            t.ok(e instanceof NotFound);
        }
    }),
);

test(
    "TableQuery#singleOrNull",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        {
            const row = await TableQuery.query(pool, q => q.singleOrNull(
                OneTableDescriptor,
                { id: 2 },
            ));

            t.deepEqual(row, { id: 2, name: "two" });
        }

        {
            const row = await TableQuery.query(pool, q => q.singleOrNull(
                OneTableDescriptor,
                { id: 4 },
            ));

            t.equal(row, null);
        }
    }),

);

test(
    "TableQuery#multiple",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        const rows = await TableQuery.query(pool, q => q.multiple(
            OneTableDescriptor,
            { id: 2 },
        ));

        t.deepEqual(rows, [{ id: 2, name: "two" }]);
    }),
);

test(
    "TableQuery#insert",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        const row = await TableQuery.query(pool, q => q.insert(
            OneTableDescriptor,
            { name: "three" },
        ));

        t.deepEqual(row, { id: 3, name: "three" });
    }),
);

test(
    "TableQuery#update",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        {
            const row = await TableQuery.query(pool, q => q.update(
                OneTableDescriptor,
                { name: "one" },
                { name: "een" },
            ));

            t.deepEqual(row, { id: 1, name: "een" });
        }

        try {
            const row = await TableQuery.query(pool, q => q.update(
                OneTableDescriptor,
                { name: "one" },
                { name: "een" },
            ));

            t.fail();
        }
        catch (e) {
            t.ok(e instanceof NotFound);
        }
    }),
);

test(
    "TableQuery#upsert",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        const row = await TableQuery.query(pool, q => q.upsert(
            OneTableDescriptor,
            { id: 2 },
            { name: "twee" },
        ));

        t.deepEqual(row, { id: 2, name: "twee" });
    }),
);

test(
    "TableQuery#ensure",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        const row = await TableQuery.query(pool, q => q.ensure(
            OneTableDescriptor,
            { id: 4 },
            { name: "four" },
        ));

        t.deepEqual(row, { id: 4, name: "four" });
    }),
);

test(
    "TableQuery#delete",
    async t => using(DatabaseTestContext.create(sql), async ({ pool }) => {
        const row = await TableQuery.query(pool, q => q.delete(
            OneTableDescriptor,
            { id: 2 },
        ));

        t.deepEqual(row, { id: 2, name: "two" });
    }),
);
