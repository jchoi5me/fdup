use colmac::*;
use rayon::prelude::*;
use sha2::Digest;
use sha2::Sha512;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use walkdir::DirEntry;
use walkdir::WalkDir;

/// Calculate the checksum of a file.
///
/// # Parameters
/// - `path`: path to the file whose contents will be used for to calculate the checksum
///
/// # Returns
/// sha512 checksum of the contents of the file
pub fn checksum(path: &Path) -> Result<Vec<u8>, Option<String>> {
    let mut hasher = Sha512::default();
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(err) => return Err(Some(format!("{}", err))),
    };
    let mut buffer = [0; 131072]; // read this much at a time

    // feed the hasher one buffer's worth at a time
    loop {
        match file.read(&mut buffer) {
            Ok(size) if size == 0 => break,            // done reading
            Ok(size) => hasher.input(&buffer[..size]), // feed the hasher
            Err(err) => panic!("failed reading {:?} to buffer {}", path, err), // undecided
        };
    }

    Ok(hasher.result().as_slice().to_vec())
}

/// # Returns
///
/// Size of the file in bytes if it is a regular file, `Err(None)` if it is not a regular file,
/// `Err(Some(_))` otherwise.
pub fn filesize(entry: &DirEntry) -> Result<usize, Option<String>> {
    match entry.metadata() {
        Ok(meta) if meta.is_file() => Ok(meta.len() as usize),
        Ok(_) => Err(None), // not a file, so skip
        Err(err) => Err(Some(format!("{}", err))),
    }
}
/// # Parameters
///
/// 1. `key_f` -- some function that maps a borrowed form of `T` into `Result<K, Option<String>>`,
///    where output values `Err(None)` are ignored, and `Err(Some(_))` are printed before being
///    ignored
/// 1. `threshold` -- only `Vec`'s with length `> theshold` are included in the returned iterator
/// 1. `items` -- the items to uses
///
/// ## Types
///
/// 1. `B` -- some type such that `T` implements `Borrow<B>`
/// 1. `F` -- mapping from `&B` to some wrapper type around `K`
/// 1. `K` -- key values uesd to group `items`
/// 1. `T` -- items being grouped
///
/// # Returns
///
/// `Iterator` of `Vec`'s, `I = [v1, v2, ...]`, such that two elements `t1` and `t2` are in the same
/// set `vi` if and only if `key_f(&t1) == key_f(&t2)`. Put another way, each set `v` is
/// characterized by a unique output value `o` of `key_f`, and `key_f` maps each element in `v` to
/// the unique `o` of that set.
pub fn disjoint_by_filter_map<B, F, K, T>(
    key_f: &F,
    threshold: usize,
    items: &Vec<T>,
) -> impl Iterator<Item = Vec<T>>
where
    B: ?Sized,
    F: Fn(&B) -> Result<K, Option<String>> + Send + Sync,
    K: Clone + Eq + Hash + Send + Sync,
    T: Borrow<B> + Clone + Debug + Send + Sync,
{
    // map input items to key values in parallel
    // collect, because hashmap construction needss to be sequential
    //
    // the hashmap holds `O(n)` anyway, so holding things in this intermediate set doesn't
    // really change things too much
    let k_to_t_mapping: Vec<(K, T)> = items
        .par_iter()
        .filter_map(|item| match key_f(item.borrow()) {
            Ok(key_res) => Some((key_res, item.clone())), // good to go
            Err(None) => None,                            // err, but don't print anything
            Err(Some(err)) => {
                eprintln!("ERROR with {:?}: {}", item, err); // err, report error
                None
            }
        })
        .collect();
    let mapping_len = k_to_t_mapping.len(); // for preallocation purposes

    // use hashmap to group things by the key values
    k_to_t_mapping
        .into_iter()
        .fold(
            HashMap::<K, Vec<T>>::with_capacity(mapping_len),
            |mut acc, (key, item)| {
                // if acc[key] is None, acc[key] = vec![item], else acc[key].push(item)
                match acc.get_mut(&key) {
                    Some(vec) => vec.push(item),
                    None => {
                        let val = vec![item];
                        acc.insert(key, val);
                    }
                };
                acc
            },
        )
        .into_iter()
        .map(|(_, v)| v)
        .filter(move |v| v.len() > threshold)
}

pub fn duplicate_files(sort_vec: bool, path: &Path) -> impl Iterator<Item = Vec<PathBuf>> {
    // get all files, ignoring all errors
    let files: Vec<_> = WalkDir::new(&path)
        .into_iter()
        .filter_map(Result::ok)
        .collect();

    // 1. group files by filesize first, discarding sets with size <= 1
    // 2. within each group, group items by checksum, discarding sets with size <= 1
    // 3. print each one as json
    disjoint_by_filter_map(&filesize, 1, &files)
        .map(|vec| vec.into_iter().map(DirEntry::into_path).collect())
        .flat_map(|set| disjoint_by_filter_map(&checksum, 1, &set))
        .map(move |vec| match sort_vec {
            true => sorted!(vec),
            false => vec,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;
    use std::collections::HashSet;
    use std::env;
    use std::fmt::Display;
    use std::fs::create_dir_all;
    use std::fs::read_to_string;
    use std::fs::remove_dir_all;
    use std::fs::remove_file;
    use std::fs::File;
    use std::io;
    use std::io::Write;
    use std::path::PathBuf;

    /// # Returns
    ///
    /// Path to some newly created tempfile in some OS-managed tempdir. The basename of this file
    /// will be prefixed with `prefix` and its content equal to `content`.
    ///
    /// Whether or not the resulting path is unique depends on the prefix.
    fn mktemp<D>(prefix: &str, content: &D) -> io::Result<PathBuf>
    where
        D: Display,
    {
        // make sure that a file does not exist under our path
        let basename = PathBuf::from(prefix);
        let path_to_temp = env::temp_dir().as_path().join(&basename);
        if path_to_temp.exists() {
            remove_file(&path_to_temp)?;
        }

        let mut file = File::create(&path_to_temp)?;
        write!(&mut file, "{}", content).unwrap(); // write untrimmed content
        Ok(path_to_temp)
    }

    fn test_data() -> Vec<String> {
        vec![
            "",
            "12asdopjkzx",
            " 12p oka0sd k\n rn12w\r\r\n \t asof AWSDJO !@# @$ ",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    #[test]
    fn parametrized_checksum() {
        let sums: HashSet<Vec<u8>> = test_data()
            .into_par_iter()
            .enumerate()
            .map(|(index, content)| {
                let prefix = format!("{}_{}_{}_{}", module_path!(), line!(), column!(), index);
                let path_to_temp = mktemp(&prefix, &content).unwrap();
                assert_eq!(content, read_to_string(&path_to_temp).unwrap());

                // pseudo check that the function is deterministic
                let sums: HashSet<Vec<u8>> =
                    (0..4).map(|_| checksum(&path_to_temp).unwrap()).collect();
                assert_eq!(1, sums.len());
                sums.into_iter().nth(0).unwrap()
            })
            .collect();
        assert_eq!(test_data().len(), sums.len());
    }

    #[test]
    fn parametrized_filesize() {
        test_data()
            .into_par_iter()
            .enumerate()
            .for_each(|(index, content)| {
                let prefix = format!("{}_{}_{}_{}", module_path!(), line!(), column!(), index);
                let path_to_temp = mktemp(&prefix, &content).unwrap();
                assert_eq!(content, read_to_string(&path_to_temp).unwrap());
                let temp_as_entry = WalkDir::new(&path_to_temp)
                    .into_iter()
                    .filter_map(Result::ok)
                    .nth(0)
                    .unwrap();
                let result = filesize(&temp_as_entry).unwrap();
                let expected = content.len();
                assert_eq!(expected, result);
            });
    }

    #[test]
    fn fdup() {
        let prefix = format!("{}_{}_{}", module_path!(), line!(), column!());
        let test_dir = std::env::temp_dir().join(&prefix);
        if test_dir.exists() {
            remove_dir_all(&test_dir).unwrap();
        }
        create_dir_all(test_dir.join("d1/d2/d3/d4")).unwrap();

        // create and populate each file
        vec![
            ("d1/f1", ""),
            ("d1/f2", ""),
            ("d1/d2/f3", "a\nbc2"),
            ("d1/d2/d3/f4", "abcde"),
            ("d1/d2/d3/d4/f5", "a\nbc2"),
        ]
        .into_par_iter()
        .map(|(path, content)| (test_dir.join(path), content))
        .for_each(|(path_buf, content)| {
            let file = File::create(&path_buf).unwrap();
            write!(&file, "{}", content).unwrap();
            assert_eq!(content, read_to_string(&path_buf).unwrap());
        });

        let results: HashSet<Vec<PathBuf>> = duplicate_files(false, &test_dir)
            .map(|v| sorted!(v))
            .collect();
        let expected = hashset![
            sorted!(vec![test_dir.join("d1/f1"), test_dir.join("d1/f2")]),
            sorted!(vec![
                test_dir.join("d1/d2/d3/d4/f5"),
                test_dir.join("d1/d2/f3")
            ])
        ];
        assert_eq!(expected, results);
    }
}
