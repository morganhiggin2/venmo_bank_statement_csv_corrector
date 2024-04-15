use std::env;
use std::{fs, io};
use polars::frame::DataFrame;
use polars::prelude::{CsvReader, CsvWriter};
use std::path::PathBuf;

const INPUT_DIR: &str = "input";

fn main() {
    read_input_files();
}

/*

1. read file from input
2. read file that keeps track of existing venmo transaction ids
3. correct file
4. output file 
*/

/*
read input file
*/
fn read_input_files() -> Result<Vec<DataFrame>, String>{
    //read files in input directory
    let input_files = match read_input_dir() {
        Ok(some) => some,
        Err(e) => {
            return Err(format!("Could not list files in input directory: e"));
        }
    };

    //list of parsed json file contents
    let dataframes: Vec<DataFrame> = Vec::with_capacity(input_files.len()); 

    //for each file
    for file_path in input_files {
        //TODO Delete
        let file_path_string = file.into_os_string().into_string().unwrap();
        println!("{file_path_string}");

        //build polars csv reader
        let csv_reader = CsvReader::from_path(file_path)?
            .with_skip_rows(4);
    }
        //read contents

        //convert contents to loosely structured json

        //append to list of file json contents

    return Ok(dataframes);
}

fn read_input_dir() -> io::Result<Vec<PathBuf>> {
    //get directory path to read input files
    let mut read_dir_path: String = "./".to_string(); 
    read_dir_path.push_str(INPUT_DIR);

    //read files in input directory
    let files_in_dir = fs::read_dir(read_dir_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    return io::Result::Ok(files_in_dir);
}