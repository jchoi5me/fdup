//! # fdup
//!
//! `fdup` finds duplicate files
//!
//! ## Example Usage
//!
//! ```bash
//! $ ./fdup .
//! ["./a.txt.1","./a.txt.2","./a.txt.3"]
//! ["./b.txt.1","./b.txt.2"]
//! ["./c.txt.1","./c.txt.2","./c.txt.3","./c.txt.4"]
//! ```
use rayon::prelude::*;
use sha2::{Digest, Sha512};
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::Read;
use walkdir::WalkDir;

/// Calculate the checksum of a file.
///
/// # Parameters
/// - `path`: path to the file whose contents will be used for to calculate the checksum
///
/// # Returns
/// sha512 checksum of the contents of the file
fn checksum(path: &String) -> Vec<u8> {
    let mut hasher = Sha512::default();
    let mut file = File::open(path).unwrap();
    let mut buffer = [0; 4096]; // read this much at a time

    // feed the hasher one buffer's worth at a time
    loop {
        match file.read(&mut buffer) {
            Ok(size) if size == 0 => break,                        // read doc
            Ok(size) if size > 0 => hasher.input(&buffer[..size]), // feed the hasher
            _ => panic!("failed reading {} to buffer", path),      // undecided
        };
    }

    // returns &[u8], so return as vec
    hasher.result().as_slice().to_vec()
}

/// Find disjoint sets whose members are paths to files with an identical checksum.
///
/// # Parameters
/// - `paths`: paths to the files to consider
///
/// # Returns
/// Disjoint sets `[s1, s2, ...]` where each set `s` contains filepaths to files
/// whose checksums are identical. In other words, given two files `f1` and `f2`,
/// `checksum(f1) == checksum(f2)` is they are in the same set, `!=` otherwise.
fn uniq_via_checksum(paths: &Box<HashSet<String>>) -> impl Iterator<Item = Box<HashSet<String>>> {
    // the hashmap to drive it all
    let mut checksum_to_files: HashMap<Vec<u8>, Box<HashSet<String>>> = HashMap::new();

    // calculate checksums in parallel
    let checksums: Vec<(&String, Vec<u8>)> = (*paths)
        .par_iter()
        .map(|path| (path, checksum(path)))
        .collect();

    // construct the map
    checksums.iter().for_each(|(path, sum)| {
        if !checksum_to_files.contains_key(sum) {
            let key = sum.clone();
            let val = Box::new(HashSet::new());
            checksum_to_files.insert(key, val);
        }

        // insert into the set in map
        checksum_to_files
            .get_mut(sum)
            .unwrap()
            .insert((*path).clone());
    });

    // return the values as an iter
    checksum_to_files.into_iter().map(|(_, val)| val)
}

/// Find disjoint sets whose members are paths to files with an identical size.
///
/// # Parameters
/// - `path`: the path to the root, from which to recursively search
///
/// # Returns
/// Disjoint sets `[s1, s2, ...]` where each set `s` contains filepaths to files
/// whose size in bytes are identical. In other words, given two files `f1` and `f2`,
/// `bytes(f1) == bytes(f2)` is they are in the same set, `!=` otherwise.
fn disjoint_size_sets(path: &str) -> impl Iterator<Item = Box<HashSet<String>>> {
    // hashmap to drive it all
    let mut size_to_files = HashMap::new();

    // recursive filepath iteration
    WalkDir::new(path)
        .into_iter()
        .map(|e| e.unwrap())
        .filter(|e| e.metadata().unwrap().is_file()) // only handle files
        .map(|e| {
            let filename = e.path().to_str().unwrap().to_owned();
            let num_bytes = e.metadata().unwrap().len();
            (filename, num_bytes)
        })
        .for_each(|(filename, num_bytes)| {
            // construct the map
            if !size_to_files.contains_key(&num_bytes) {
                let val = Box::new(HashSet::new());
                size_to_files.insert(num_bytes, val);
            }
            // insert into the map
            size_to_files
                .get_mut(&num_bytes)
                .unwrap()
                .insert(filename.clone());
        });

    // only keep sets with > 1 members
    size_to_files.retain(|_, v| v.len() > 1);

    // return values as an iter
    size_to_files.into_iter().map(|(_, val)| val)
}

fn main() {
    // parse args, TODO use clargparser
    let args: Vec<_> = env::args().collect();
    let root = match args.get(1) {
        Some(path) => path,
        _ => panic!("first arg should be the root of the search"),
    };

    let mut conflict_sets: Vec<Vec<String>> = disjoint_size_sets(root)
        .flat_map(|set| uniq_via_checksum(&set)) // sets of colliding checksums
        .filter(|set| set.len() > 1) // only those that actually collide
        .map(|set| set.iter().cloned().collect::<Vec<String>>()) // as vec, for printing
        .map(|mut vec| {
            vec.sort();
            vec
        })
        .collect();

    // largest sets in the front
    // TODO support --max --min options
    // TODO optionally sort in the future, keeping conflcit sets as an iter
    conflict_sets.sort_by(|a, b| b.len().cmp(&a.len()));

    // print each on a separate line, json encoded
    conflict_sets.iter().for_each(|vec| println!("{:?}", vec));
}
