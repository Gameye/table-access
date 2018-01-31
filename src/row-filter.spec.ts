import * as test from "blue-tape";
import {
    makeRowFilterFunction, makeRowFilterPg, normalizeRowFilter,
} from "./row-filter";

test("row-filter normalize", async (t) => {
    t.deepEqual(normalizeRowFilter({ a: null }), {
        _ft: "and",
        filter: [{
            _ft: "eq",
            field: "a",
            value: null,
        }],
    });
});

test("row-filter pg eq", async (t) => {

    t.deepEqual(makeRowFilterPg({
        _ft: "eq", field: "a", value: null,
    }, "t"), {
            filterSql: `"t"."a" IS NULL`,
            param: [],
            paramCount: 0,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "eq", field: "a", value: null, invert: true,
    }, "t", 10), {
            filterSql: `"t"."a" IS NOT NULL`,
            param: [],
            paramCount: 0,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "eq", field: "a", value: 0,
    }, "t"), {
            filterSql: `"t"."a" = $1`,
            param: [0],
            paramCount: 1,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "eq", field: "a", value: "", invert: true,
    }, "t", 10), {
            filterSql: `"t"."a" <> $11`,
            param: [""],
            paramCount: 1,
        });

});

test("row-filter pg min max", async (t) => {

    t.deepEqual(makeRowFilterPg({
        _ft: "min", field: "a", value: 0,
    }, "t"), {
            filterSql: `"t"."a" >= $1`,
            param: [0],
            paramCount: 1,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "min", field: "a", value: "", exclusive: true,
    }, "t", 10), {
            filterSql: `"t"."a" > $11`,
            param: [""],
            paramCount: 1,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "max", field: "a", value: "",
    }, "t"), {
            filterSql: `"t"."a" <= $1`,
            param: [""],
            paramCount: 1,
        });

    t.deepEqual(makeRowFilterPg({
        _ft: "max", field: "a", value: 0, exclusive: true,
    }, "t", 10), {
            filterSql: `"t"."a" < $11`,
            param: [0],
            paramCount: 1,
        });

});

test("row-filter pg and or", async (t) => {

    t.deepEqual(
        makeRowFilterPg({
            _ft: "and",
            filter: [{
                _ft: "eq",
                field: "a",
                value: "",
            }],
        }, "t"),
        {
            filterSql: `"t"."a" = $1`,
            param: [""],
            paramCount: 1,
        },
    );

    t.deepEqual(
        makeRowFilterPg<{ a: string, b: number }>({
            _ft: "and",
            filter: [{
                _ft: "eq",
                field: "a",
                value: "",
            }, {
                _ft: "eq",
                field: "b",
                value: 0,
            }],
        }, "t", 20),
        {
            filterSql: `("t"."a" = $21 AND "t"."b" = $22)`,
            param: ["", 0],
            paramCount: 2,
        },
    );

    t.deepEqual(
        makeRowFilterPg<{ a: string, b: number }>({
            _ft: "or",
            filter: [{
                _ft: "eq",
                field: "a",
                value: "yes",
            }, {
                _ft: "and",
                filter: [{
                    _ft: "min",
                    field: "b",
                    value: -10,
                }, {
                    _ft: "max",
                    field: "b",
                    value: 10,
                    exclusive: true,
                }],
            }],
        }, "t"),
        {
            filterSql: `("t"."a" = $1 OR ("t"."b" >= $2 AND "t"."b" < $3))`,
            param: ["yes", -10, 10],
            paramCount: 3,
        },
    );

    t.deepEqual(
        makeRowFilterPg<{ a: string, b: number | null }>({
            _ft: "or",
            filter: [{
                _ft: "eq",
                field: "a",
                value: "yes",
            }, {
                _ft: "and",
                filter: [{
                    _ft: "min",
                    field: "b",
                    value: -10,
                }, {
                    _ft: "max",
                    field: "b",
                    value: 10,
                    exclusive: true,
                }],
            }, {
                a: "ok",
            }, {
                a: "cool",
                b: -100,
            }, {
                b: null,
            }],
        }, "t"),
        {
            filterSql: `(` +
                `"t"."a" = $1 OR ` +
                `("t"."b" >= $2 AND "t"."b" < $3) OR ` +
                `"t"."a" = $4 OR ` +
                `("t"."a" = $5 AND "t"."b" = $6) OR ` +
                `"t"."b" IS NULL` +
                `)`,
            param: ["yes", -10, 10, "ok", "cool", -100],
            paramCount: 6,
        },
    );

});

test("row-filter fn eq", async (t) => {
    const list = [
        { a: null },
        { a: 0 },
        { a: 10 },
        { a: 11 },
    ];

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "eq", field: "a", value: null }),
        ),
        [
            { a: null },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "eq", field: "a", value: null, invert: true }),
        ),
        [
            { a: 0 },
            { a: 10 },
            { a: 11 },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "eq", field: "a", value: 0 }),
        ),
        [
            { a: 0 },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "eq", field: "a", value: 0, invert: true }),
        ),
        [
            { a: null },
            { a: 10 },
            { a: 11 },
        ],

    );

});

test("row-filter fn min max", async (t) => {
    const list = [
        { a: null },
        { a: 0 },
        { a: 10 },
        { a: 11 },
    ];

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "min", field: "a", value: 0 }),
        ),
        [
            { a: 0 },
            { a: 10 },
            { a: 11 },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "min", field: "a", value: 0, exclusive: true }),
        ),
        [
            { a: 10 },
            { a: 11 },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "max", field: "a", value: 10 }),
        ),
        [
            { a: 0 },
            { a: 10 },
        ],

    );

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null }>({ _ft: "max", field: "a", value: 10, exclusive: true }),
        ),
        [
            { a: 0 },
        ],

    );

});

test("row-filter fn and or", async (t) => {

    const list = [
        { a: null, b: "yes" },
        { a: 0, b: "ok" },
        { a: 10, b: "cool" },
        { a: 11, b: "ok" },
    ];

    t.deepEqual(
        list.filter(
            makeRowFilterFunction<{ a: number | null, b: string }>({
                _ft: "or",
                filter: [{
                    _ft: "and",
                    filter: [{
                        _ft: "min",
                        field: "a",
                        value: 10,
                        exclusive: true,
                    }, {
                        _ft: "max",
                        field: "a",
                        value: 20,
                    }],
                }, {
                    b: "ok",
                }, {
                    b: "cool",
                    a: -100,
                }, {
                    a: null,
                }],
            }),
        ),
        [
            { a: null, b: "yes" },
            { a: 0, b: "ok" },
            { a: 11, b: "ok" },
        ],

    );

});
