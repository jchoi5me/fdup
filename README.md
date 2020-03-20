# trim

[![trim-crate](https://img.shields.io/crates/v/trim.svg)](https://crates.io/crates/trim)

- [crates.io](https://crates.io/crates/trim)

`trim` was inspired by the auto-trimming feature of [Atom](https://atom.io/), where trailing whitespace of every line is removed, and trailing newlines are replaced with a single newline.

## Table of Contents

1. [trim](#trim)
    1. [Overview](#overview)

## Overview

```bash
~
  $ trim --help
trim 1.2.1
Jack <jackwchoi@pm.me>
trim whitespaces from files

USAGE:
    trim [FLAGS] [files]...

FLAGS:
    -h, --help               Prints help information
    -i, --in-place           trim <files> in-place, overwritting the content of the files atomically
    -N, --supress-newline    suppress outputting the trailing `\n` in the last line
    -S, --supress-summary    suppress summary
    -V, --supress-visual     suppress visualizations of the trim
        --version            Prints version information

ARGS:
    <files>...    files to trim; if '-' exists or none provided, stdin will be used
```
