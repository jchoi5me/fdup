use structopt::StructOpt;

mod clargs;
mod fdup;
mod util;

use clargs::*;
use fdup::*;

fn main() {
    let Opt { root } = Opt::from_args();

    duplicate_files(&root).for_each(|vec| println!("{:?}", vec));
}
