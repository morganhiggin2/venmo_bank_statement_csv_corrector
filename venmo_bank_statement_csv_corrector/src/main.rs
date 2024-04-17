use std::env;
use std::fmt::write;
use std::process::Output;
use std::{fs::{File, read_dir}, io};
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::{CsvReader, CsvWriter};
use std::path::PathBuf;

const INPUT_DIR: &str = "input";
const OUTPUT_DIR: &str = "output";

fn main() {
    let dataframes = read_input_files().unwrap();
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
        let file_path_string = file_path.clone().into_os_string().into_string().unwrap();

        //build polars csv reader
        let csv_reader = match CsvReader::from_path(file_path) {
            Ok(builder) => {
                builder
                .with_skip_rows(2)
                .has_header(true)
            }
            Err(e) => {
                return Err(format!("Error during building csv reader: {}", e.to_string()));
            }
        };

        //parse csv file
        let mut dataframe = match csv_reader.finish() {
            Ok(some) => some,
            Err(e) => {
                return Err(format!("Error during parsing csv file {} with error {}", file_path_string, e.to_string()));
            }
        };

        //create output file path
        let mut output_file = get_output_file_buffer("output_file.csv".to_string())?; 

        //build csv file writer
        let mut csv_writer = CsvWriter::new(&mut output_file)
            .include_header(true)
            .with_datetime_format(Some("".to_string()));

        //write csv file to output location
        match csv_writer.finish(&mut dataframe) {
            Ok(_) => (),
            Err(e) => {
                return Err(format!("Error writing dataframe to output file {}", e.to_string()));
            }
        };
    }

    return Ok(dataframes);
}

/* Read input directory, get list of files in input directory */
fn read_input_dir() -> io::Result<Vec<PathBuf>> {
    //get directory path to read input files
    let mut read_dir_path: String = "./".to_string(); 
    read_dir_path.push_str(INPUT_DIR);

    //read files in input directory
    let files_in_dir = read_dir(read_dir_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    return io::Result::Ok(files_in_dir);
}

/*
create output file buffer
*/
fn get_output_file_buffer(output_file_name: String) -> Result<File, String> {
    //get output file path
    let mut write_file_path: String = "./".to_string(); 
    write_file_path.push_str(OUTPUT_DIR);
    write_file_path.push_str("/");
    write_file_path.push_str(&output_file_name);

    //write file
    let output_file: File = match File::create(write_file_path) {
        Ok(some) => some,
        Err(e) => {
            return Err(format!("Error creating output file: {}", e));
        }
    };

    return Ok(output_file);
}
//TODO if directory does not exist, create it