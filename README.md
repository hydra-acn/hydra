# Disclaimer

This repository includes the research prototypes for the anonymous communication network Hydra.
If you are viewing this on `github`:
Submodules and other related repositories can be found [here](https://github.com/hydra-acn).

The main concepts/ideas of Hydra are published in the following conference paper:

> Schatz, David; Rossberg, Michael; Schaefer, Guenter. Hydra: Practical Metadata Security for Contact Discovery,Messaging, and Dialing. ICISSP, 2021.

Apart from the concepts described in the paper, the prototype code lacks a lot of documentation at the moment. Some more details are specified in the `protobuf` submodule (as comments for the `gRPC` definitions).

:warning: **Deploy and use the prototype at own risk.**

# Submodules

Do not forget to update the submodules on a regular basis (after `clone` and after `pull` if necessary):

```
git submodule update --init --recursive --remote --merge
```

Note if you are viewing this on the `github` website:
Links to the submodules do not seem to work from the website, but the command line will work.

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

# Deploy

First, launch the directory service. More information can be found using the command line, e.g.:
```
target/release/directory_service --help
```

Then, launch the mixes. Again, more information can be found using the command line, e.g.:
```
target/release/mix --help
```

Coming soon: Prototype Android Client
