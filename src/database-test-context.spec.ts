import * as test from "blue-tape";
import { using } from "using-disposable";
import { DatabaseTestContext } from "./database-test-context";

const sql = `
CREATE TABLE public.one(
    id SERIAL,
    name TEXT NOT NULL
);
`;

test(
    "DatabaseTestContext#create",
    async t => using(DatabaseTestContext.create(sql), async ({ tempPool }) => {
        const result = await tempPool.query(`
INSERT INTO public.one (name)
VALUES('one')
RETURNING *
;`);
        t.equal(result.rowCount, 1);

        const [row] = result.rows;
        t.deepEqual(row, { id: 1, name: "one" });
    }),
);
