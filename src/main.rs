use structopt::StructOpt;

mod clargs;
mod fdup;
mod util;

use clargs::*;
use fdup::*;

fn main() {
    let Opt { sort_vec, root } = Opt::from_args();

    duplicate_files(sort_vec, &root).for_each(|vec| println!("{:?}", vec));
}
