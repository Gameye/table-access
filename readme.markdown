table-access is a library that allows for simple access to PostgreSQL data in
real time. You may subscribe to a table with a filter and everytime a row in
that table is inserted, updated or deleted you will be notified.

Also this library supports easy transactionale manipluation of data via the
same filter API.

Also this library has a component (DatabaseTestContext) that may be used for
unit-(ish) testing your database or using your datbase as a fixture for unit
testing something else (you will never need to mock your databas again!!!). 

# automated tests
NEVER commit something that breaks the build! If you do, you suck. You can
easily prevent this by linking the `test.sh` script as a git `pre-push` or
`pre-commit` hook!

like this:
```bash
ln test.sh .git/hooks/pre-commit
```

If you use a git commit hook for testing, you may also bypass this hook with
the `--no-verify` or `-n` option of git commit, like this:
```bash
git commit -nm'some commit message'
```
