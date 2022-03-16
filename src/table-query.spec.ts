import test from "tape-promise/tape.js";
import { using } from "dispose";
import { PgContext } from "pg-context";
import { RowDescriptor } from "./row-descriptor";
import { streamWait } from "./stream-wait";
import { TableQuery } from "./table-query";

const sql = `
CREATE FUNCTION public.notify_trg()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $BODY$
BEGIN
    PERFORM pg_notify(TG_ARGV[0], json_build_object(
        'op', TG_OP,
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'old', CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN row_to_json(OLD) ELSE NULL END,
        'new', CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END
    )::text);

    RETURN NEW;
END;
$BODY$;

CREATE TABLE public.one(
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO public.one(name)
VALUES('one'), ('two');

CREATE TRIGGER notify_trg
AFTER INSERT OR UPDATE OR DELETE
ON public.one
FOR EACH ROW
EXECUTE PROCEDURE public.notify_trg('row');
`;

interface OneRow {
    id: number;
    name: string;
}

const OneRowDescriptor: RowDescriptor<OneRow> = {
    schema: "public",
    table: "one",
};

test("TableQuery", async t => using(PgContext.create(sql), async ({ pool }) => {
    const query = new TableQuery(pool, "row", [{
        row: OneRowDescriptor,
        filter: {
            _ft: "or",
            filter: [
                { name: "two" },
                { name: "four" },
            ],
        },
    }]);

    {
        const event = streamWait(query, () => true);
        t.deepEqual(await event, {
            type: "initial",
            row: OneRowDescriptor,
            initial: [
                { id: 2, name: "two" },
            ],
        });
    }

    {
        const event = streamWait(query, () => true);
        await pool.query(`
INSERT INTO public.one(name)
VALUES('three'), ('four')
`);
        t.deepEqual(await event, {
            type: "change",
            row: OneRowDescriptor,
            new: { id: 4, name: "four" },
            old: null,
        });
    }

    {
        const event = streamWait(query, () => true);
        await pool.query(`
UPDATE public.one
SET name = 'four'
WHERE id = 1
`);
        t.deepEqual(await event, {
            type: "change",
            row: OneRowDescriptor,
            new: { id: 1, name: "four" },
            old: null,
        });
    }

    {
        const event = streamWait(query, () => true);
        await pool.query(`
DELETE FROM public.one
WHERE id = 1
`);
        t.deepEqual(await event, {
            type: "change",
            row: OneRowDescriptor,
            new: null,
            old: { id: 1, name: "four" },
        });
    }

    await new Promise(
        (resolve, reject) => query.
            once("close", resolve).
            once("error", reject).
            destroy(),
    );
}));
