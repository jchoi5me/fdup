use sha2::Digest;
use sha2::Sha512;
use std::fs::metadata;
use std::fs::File;
use std::io::Read;
use std::path::Path;

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
pub fn filesize(path: &Path) -> Result<usize, Option<String>> {
    match metadata(path) {
        Ok(meta) if meta.is_file() => Ok(meta.len() as usize),
        Ok(_) => Err(None), // not a file, so skip
        Err(err) => Err(Some(format!("{}", err))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;
    use std::collections::HashSet;
    use std::env;
    use std::fmt::Display;
    use std::fs::read_to_string;
    use std::fs::remove_file;
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
                let result = filesize(&path_to_temp).unwrap();
                let expected = content.len();
                assert_eq!(expected, result);
            });
    }
}
