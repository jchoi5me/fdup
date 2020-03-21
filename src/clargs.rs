use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "fdup")]
pub struct Opt {
    /// Sort each vector lexicographically
    #[structopt(short = "s", long = "sort-vec")]
    pub sort_vec: bool,

    /// Root directory from which to start the search
    #[structopt(parse(from_os_str))]
    pub root: PathBuf,
}
