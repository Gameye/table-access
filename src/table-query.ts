import * as pg from "pg";
import { Readable } from "stream";
import { RowDescriptor } from "./row-descriptor";
import { makeRowFilterFunction, makeRowFilterPg, RowFilter } from "./row-filter";

export type TableQueryEvent<TRow extends object> =
    TableQueryInitialEvent<TRow> |
    TableQueryChangeEvent<TRow>;

export interface TableQueryInitialEvent<TRow extends object> {
    type: "initial";
    row: RowDescriptor<TRow>;
    initial: TRow[];
}

export interface TableQueryChangeEvent<TRow extends object> {
    type: "change";
    row: RowDescriptor<TRow>;
    old: TRow | null;
    new: TRow | null;
}

export interface QueryDescriptor<TRow extends object> {
    row: RowDescriptor<TRow>;
    filter: RowFilter<TRow> | Partial<TRow>;
}

interface RowTriggerEvent<TRow extends object> {
    op: "INSERT" | "UPDATE" | "DELETE";
    schema: string;
    table: string;
    old: TRow | null;
    new: TRow | null;
}

export class TableQuery<TRow extends object> extends Readable {

    private setupCalled = false;
    private teardownCalled = false;

    constructor(
        private readonly client: pg.ClientBase,
        private readonly channel: string,
        private readonly queryDescriptors: Array<QueryDescriptor<TRow>>,
    ) {
        super({ objectMode: true });
    }

    public _read(size: number): void {
        if (this.setupCalled) return;
        this.setup().catch(error => this.emit("error", error));
    }

    public _destroy(
        destroyError: Error | null,
        callback: (error: Error | null) => void,
    ): void {
        if (this.teardownCalled) return;
        this.teardown().
            then(
                () => callback(destroyError),
                error => callback(destroyError || error),
            );
    }

    private async setup() {
        this.setupCalled = true;

        const { client, channel, handleNotificationEvent, queryDescriptors } = this;

        try {
            await client.query(`BEGIN TRANSACTION;`);

            for (const { row, filter } of queryDescriptors) {
                if (this.teardownCalled) return;

                const rows = await this.fetch(row, filter);
                this.push({
                    type: "initial",
                    row,
                    initial: rows,
                } as TableQueryInitialEvent<TRow>);
            }

            await client.query(`LISTEN ${client.escapeIdentifier(channel)}`);
            client.addListener("notification", handleNotificationEvent);

            await client.query(`COMMIT TRANSACTION;`);
        }
        catch (error) {
            await client.query(`ROLLBACK TRANSACTION;`);
            throw error;
        }
    }

    private async teardown(
    ) {
        this.teardownCalled = true;

        const { client, channel, handleNotificationEvent } = this;

        client.removeListener("notification", handleNotificationEvent);
        await client.query(`UNLISTEN ${client.escapeIdentifier(channel)}`);

        this.push(null);
    }

    private handleNotificationEvent = ({
        channel, payload,
    }: pg.Notification) => {
        const { queryDescriptors } = this;

        if (this.channel !== channel) return;
        if (!payload) return;

        let event: RowTriggerEvent<TRow> = JSON.parse(payload);
        try {
            event = JSON.parse(payload);
        }
        catch (error) {
            return;
        }

        for (const { row, filter } of queryDescriptors) {
            if (this.teardownCalled) return;

            if (event.schema !== row.schema) continue;
            if (event.table !== row.table) continue;

            const filterFunction = makeRowFilterFunction(filter);
            const newRow = event.new && filterFunction(event.new) ? event.new : null;
            const oldRow = event.old && filterFunction(event.old) ? event.old : null;

            if (!(newRow || oldRow)) return;

            this.push({
                type: "change",
                row,
                old: oldRow,
                new: newRow,
            } as TableQueryChangeEvent<TRow>);
        }

    }

    private async fetch(
        descriptor: RowDescriptor<TRow>,
        filter: RowFilter<TRow> | Partial<TRow>,
    ): Promise<TRow[]> {
        const { client } = this;

        const filterResult = makeRowFilterPg(filter, "r");
        const result = await client.query(`
SELECT row_to_json(r) AS o
FROM "${descriptor.schema}"."${descriptor.table}" AS r
${filterResult.paramCount ? `WHERE ${filterResult.filterSql}` : ""}
FOR SHARE
;`, filterResult.param);
        const { rows } = result;
        return rows.map(row => row.o);
    }

}
