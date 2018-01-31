export interface TableDescriptor<TRow extends object> {
    readonly schema: string;
    readonly table: string;
}
