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
