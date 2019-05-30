import * as test from "blue-tape";
import { using } from "dispose";
import { PgContext } from "pg-context";
import { UnexpectedRowCountError, UniqueConstraintError } from "./error";
import { RowDescriptor } from "./row-descriptor";
import { TableTransaction } from "./table-transaction";

const sql = `
CREATE TABLE public.one(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
INSERT INTO public.one(name)
VALUES('one'), ('two');
`;

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
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            {
                const row = await TableTransaction.execute(client, q => q.single(
                    OneRowDescriptor,
                    { id: 2 },
                ));

                t.deepEqual(row, { id: 2, name: "two" });
            }
            try {
                const row = await TableTransaction.execute(client, q => q.single(
                    OneRowDescriptor,
                    { id: 4 },
                ));

                t.fail();
            }
            catch (err) {
                t.ok(err instanceof UnexpectedRowCountError);
            }
        }
        finally {
            client.release();
        }
    }),
);

test(
    "TableTransaction#singleOrNull",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            {
                const row = await TableTransaction.execute(client, q => q.singleOrNull(
                    OneRowDescriptor,
                    { id: 2 },
                ));

                t.deepEqual(row, { id: 2, name: "two" });
            }

            {
                const row = await TableTransaction.execute(client, q => q.singleOrNull(
                    OneRowDescriptor,
                    { id: 4 },
                ));

                t.equal(row, null);
            }
        }
        finally {
            client.release();
        }

    }),

);

test(
    "TableTransaction#multiple",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            const rows = await TableTransaction.execute(client, q => q.multiple(
                OneRowDescriptor,
                { id: 2 },
            ));

            t.deepEqual(rows, [{ id: 2, name: "two" }]);
        }
        finally {
            client.release();
        }

    }),
);

test(
    "TableTransaction#insert",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            {
                const row = await TableTransaction.execute(client, q => q.insert(
                    OneRowDescriptor,
                    { name: "three" },
                ));

                t.deepEqual(row, { id: 3, name: "three" });
            }

            try {
                const row = await TableTransaction.execute(client, q => q.insert(
                    OneRowDescriptor,
                    { id: 1, name: "four" },
                ));

                t.fail();
            }
            catch (err) {
                t.ok(err instanceof UniqueConstraintError);
            }

            try {
                const row = await TableTransaction.execute(client, q => q.insert(
                    OneRowDescriptor,
                    { id: 5, name: "one" },
                ));

                t.fail();
            }
            catch (err) {
                t.ok(err instanceof UniqueConstraintError);
            }
        }
        finally {
            client.release();
        }

    }),
);

test(
    "TableTransaction#update",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            {
                const row = await TableTransaction.execute(client, q => q.update(
                    OneRowDescriptor,
                    { name: "one" },
                    { name: "een" },
                ));

                t.deepEqual(row, { id: 1, name: "een" });
            }

            try {
                const row = await TableTransaction.execute(client, q => q.update(
                    OneRowDescriptor,
                    { name: "one" },
                    { name: "een" },
                ));

                t.fail();
            }
            catch (err) {
                t.ok(err instanceof UnexpectedRowCountError);
            }
        }
        finally {
            client.release();
        }

    }),
);

test(
    "TableTransaction#upsert",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            const row = await TableTransaction.execute(client, q => q.upsert(
                OneRowDescriptor,
                { id: 2 },
                { name: "twee" },
            ));

            t.deepEqual(row, { id: 2, name: "twee" });
        }
        finally {
            client.release();
        }

    }),
);

test(
    "TableTransaction#delete",
    async t => using(PgContext.create(sql), async ({ pool }) => {
        const client = await pool.connect();
        try {
            const row = await TableTransaction.execute(client, q => q.delete(
                OneRowDescriptor,
                { id: 2 },
            ));

            t.deepEqual(row, { id: 2, name: "two" });
        }
        finally {
            client.release();
        }

    }),
);
