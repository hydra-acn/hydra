# Submodules

Do not forget to update the submodules on a regular basis (after `clone` and after `pull` if necessary):

```
git submodule update --init --recursive --remote --merge
```

# Git conventions

- Use descriptive commit messages, written in present tense (English!).
  E.g. "Add unit test for ..." instead of "Adding ..." or "Added ...".
- If possible, do not commit unless everything compiles and all tests pass.
  If you commit anyway (e.g. because you want feedback), start the commit message with "WIP:" (work in progress).
- Work on your own branch and use the merge request feature of `gitlab` after you finished your task.
  Similar to commits, mark the merge request with "WIP:" if you are not done yet, but want early feedback.
- Name your branch according to the feature you implement or according to the component you write a test for.
  For the latter, `test` should also be part of the branch name.

# Coding Conventions

Use [`rustfmt`](https://github.com/rust-lang/rustfmt) with the config file `.rustfmt.toml` found in the root directory.
For example, you can run `cargo fmt` to format all files at once.
Furthermore, there are various options for integration with IDEs.

# Building and running tests

Build the project:
```
cargo build
```

Run all tests:
```
cargo test
```

After installing [Tarpaulin](https://github.com/xd009642/tarpaulin), generate a test coverage report with:
```
cargo tarpaulin --exclude-files "include/*" --exclude-files "tests/*" -o Html
```
An HTML report is generated as `tarpaulin-report.html`.

The documentation of the project and all dependencies (the latter being more useful at the moment) can be built and opened by:
```
cargo doc --open
```
