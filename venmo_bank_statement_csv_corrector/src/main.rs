use std::env;
use std::fmt::write;
use std::process::Output;
use std::collections::hash_map;
use std::{fs::{File, read_dir}, io};
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::*;
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

        //get dataframe columns
        let dataframe_columns = dataframe.get_columns();

        //create hashmap of column name to column reference 
        let mut dataframe_column_search = hash_map::HashMap::new();

        {
            //populate hashmap
            for column in dataframe_columns {
                dataframe_column_search.insert(column.name(), column);
            } 
        }

        //new dataframe with correct data
        let mut corrected_dataframe = DataFrame::empty();

        //create dataframe with column ID
        //<Series as Clone>::clone(&some)
        {
            let id_column: Series = match dataframe_column_search.get("ID") {
                Some(val) => Series::new("Tracking1", val.to_owned()),
                None => {
                    return Err("Unable to fetch column ID from original database".to_string());
                }
            };

            corrected_dataframe = match DataFrame::new(vec![id_column]) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Unable to create dataframe with Tracking1 column: {}", e));
                }
            };
        }

        //add correct date column, with correct format
        {
            //get owned date column
            let date_column = match dataframe_column_search.get("Datetime") {
                Some(val) => Series::new("Tracking1", val.to_owned()),
                None => {
                    return Err("Could not find column Datetime in original dataframe".to_string());
                }
            };

            //Only keep first 10 slices of string
            let date_column: Series = date_column
                .str()
                .unwrap() 
                .into_iter()
                .map(|chunkedarray| 
                    match chunkedarray
                    {
                        //attempt to get first 10 chars
                        Some(val) => match val.get(..10) 
                            {
                                Some(val) => val, 
                                //if none, return what we have
                                None => val
                            },
                        //if no str value, return empty string
                        None => "".into()
                    })
                .collect();

            //convert Datetime column date format, and back into str
            let date_col = match date_column.str().unwrap().as_date(Some("%Y-%m-%d"), true) {
                Ok(some) => some.into_series(),
                Err(e) => {
                    return Err(format!("Error converting Datetime column from date to strimg: {}", e));
                }
            };

            //set date column name
            let date_col = date_col.with_name("Date");

            match corrected_dataframe.insert_column(0, date_col) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Unable to insert new column Date: {}", e));
                }
            };
        }

        //TODO after transformation, seperate, merge files then do transform, then write, in seperate method calls
        //create output file path
        let mut output_file = get_output_file_buffer("output_file.csv".to_string())?; 

        //build csv file writer
        let mut csv_writer = CsvWriter::new(&mut output_file)
            .include_header(true)
            .with_datetime_format(Some("".to_string()));

        //write csv file to output location
        match csv_writer.finish(&mut corrected_dataframe) {
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