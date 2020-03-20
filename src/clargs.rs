use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "fdup")]
pub struct Opt {
    // Root directory from which to start the search
    #[structopt(parse(from_os_str))]
    pub root: PathBuf,
}
