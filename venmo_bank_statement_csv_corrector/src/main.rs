use std::env;
use std::fmt::write;
use std::process::Output;
use std::collections::hash_map;
use std::{fs::{File, read_dir}, io};
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::*;
use std::path::PathBuf;
use std::borrow::Cow;

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
        //build polars csv reader
        let csv_reader = match CsvReader::from_path(file_path.clone()) {
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
                return Err(format!("Error during parsing csv file with error {}", e.to_string()));
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

        //create dataframe with column Reference 
        {
            let id_column: Series = match dataframe_column_search.get("ID") {
                Some(val) => Series::new("Reference", val.to_owned()),
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
            let date_column = match date_column
                .str()
                .unwrap()
                .as_date(Some("%Y-%m-%d"), true) {
                    Ok(some) => some,
                    Err(e) => {
                        return Err(format!("Error converting Datetime column from date to strimg: {}", e));
                    }
                }
                .into_series();

            let date_column = date_column
                .date()
                .unwrap()
                .to_string("%d/%m/%Y")
                .into_series();

            //set date column name
            let date_column = date_column.with_name("Date");

            match corrected_dataframe.insert_column(1, date_column) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Unable to insert new column Date: {}", e));
                }
            };
        }

        //get transaction amount
        {
            //Amount (total) + Amount (tip) + Amount (tax) + Amount (fee)
            //get amount columns 
            let amount_total = match dataframe_column_search.get("Amount (total)") {
                Some(val) => Series::new("amount_total", val.to_owned()),
                None => {
                    return Err("Unable to fetch column \"Amount (tip)\" from original database".to_string());
                }
            };
            let amount_tip = match dataframe_column_search.get("Amount (tip)") {
                Some(val) => Series::new("amount_tip", val.to_owned()),
                None => {
                    return Err("Unable to fetch column \"Amount (tip)\" from original database".to_string());
                }
            };
            let amount_tax = match dataframe_column_search.get("Amount (tax)") {
                Some(val) => Series::new("amount_tax", val.to_owned()),
                None => {
                    return Err("Unable to fetch column \"Amount (tax)\" from original database".to_string());
                }
            };
            let amount_fee = match dataframe_column_search.get("Amount (fee)") {
                Some(val) => Series::new("amount_fee", val.to_owned()),
                None => {
                    return Err("Unable to fetch column \"Amount (fee)\" from original database".to_string());
                }
            };            

            //clean amount columns
            let amount_total = match clean_amount_field(amount_total) {
                Ok(some) => some,
                Err(e) => return Err(e) 
            };
            let amount_tip = match clean_amount_field(amount_tip)
            {
                Ok(some) => some,
                Err(e) => return Err(e) 
            };
            let amount_tax = match clean_amount_field(amount_tax)
            {
                Ok(some) => some,
                Err(e) => return Err(e) 
            };
            let amount_fee = match clean_amount_field(amount_fee)
            {
                Ok(some) => some,
                Err(e) => return Err(e) 
            };

            //convert to f32 type
            let amount_total = match amount_total.cast(&DataType::Float32) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not cast amount_total column to f32 type: {}", e));
                }
            };
            let amount_tip= match amount_tip.cast(&DataType::Float32) {
                Ok(some) => some,
                Err(e) => {
                    println!("here");
                    return Err(format!("Could not cast amount_tip column to f32 type: {}", e));
                }
            };
            let amount_tax = match amount_tax.cast(&DataType::Float32) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not cast amount_tax column to f32 type: {}", e));
                }
            };
            let amount_fee = match amount_fee.cast(&DataType::Float32) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not cast amount fee column to f32 type: {}", e));
                }
            };

            //replace null values in float columns with 0
            let amount_total = match amount_total.fill_null(FillNullStrategy::Zero) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not fill null values in column amount_total with 0: {}", e));
                }
            };
            
            let amount_tip = match amount_tip.fill_null(FillNullStrategy::Zero) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not fill null values in column amount_tip with 0: {}", e));
                }
            };
            
            let amount_tax = match amount_tax.fill_null(FillNullStrategy::Zero) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not fill null values in column amount_tax with 0: {}", e));
                }
            };
            
            let amount_fee = match amount_fee.fill_null(FillNullStrategy::Zero) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not fill null values in column amount_fee with 0: {}", e));
                }
            }; 

            //get correct amount
            let amount_correct = amount_total + amount_tip + amount_tax + amount_fee;

            //set column name
            let amount_correct = amount_correct.with_name("Amount");

            match corrected_dataframe.insert_column(2, amount_correct) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Error inserting column amount_total into corrected dataframe: {}", e));
                }
            };
        }

        //get payee
        {
            //get owned date column
            let payee_column = match dataframe_column_search.get("From") {
                Some(val) => Series::new("Payee", val.to_owned()),
                None => {
                    return Err("Could not find column From in original dataframe".to_string());
                }
            };

            //insert into corrected dataframe
            match corrected_dataframe.insert_column(3, payee_column) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Error inserting column Payee into corrected dataframe: {}", e));
                }
            };
        }

        //get description
        {
            //get owned date column
            let description_column = match dataframe_column_search.get("Note") {
                Some(val) => Series::new("Description", val.to_owned()),
                None => {
                    return Err("Could not find column Note in original dataframe".to_string());
                }
            };


            match corrected_dataframe.insert_column(4, description_column) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Error inserting column Description into corrected dataframe: {}", e));
                }
            };
        }

        //remove rows if their amount is 0
        {
            //get amount column
            let amount_column = match corrected_dataframe.column("Amount") {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not find column Amount in corrected dataframe {}", e));
                }
            };

            //get amount 32 chunked array references
            let amount_f32 = match amount_column.f32() {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not get f32 chunked array from column Amount: {}", e));
                }
            };

            //get mask for amount == 0.0
            let mask: ChunkedArray<BooleanType> = amount_f32.into_iter().map(|val| -> bool {
                //handle amount is nil case
                match val {
                    Some(val) => {
                        //if amount > 0, keep
                        if val != 0.0 {
                            return true 
                        } 

                        //else, get rid of 
                        return false 
                    }
                    None => {
                        return false 
                    }
                };
            })
            .collect();

            corrected_dataframe = match corrected_dataframe.filter(&mask) {
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not filter corrected dataframe with amount > 0 mask: {}", e));
                }
            }; 
        }

        //get input file name
        let file_name = match file_path.file_name() {
            Some(val) => match val.to_str() {
                Some(val) => val,
                None => {
                    return Err("Could not convert osstr to string".to_string());
                }
            },
            None => {
                return Err("Could not get file name from path buf".to_string());
            }
        };

        //create output file path
        let mut output_file = get_output_file_buffer(file_name.to_string())?; 

        //build csv file writer
        let mut csv_writer = CsvWriter::new(&mut output_file)
            .include_header(true);

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

fn clean_amount_field(column: Series) -> Result<Series, String> {
    //attempt getting string value from column, if we fail, attempt to cast to string 
    let column = match column.str() {
        Ok(_) => column,
        Err(_) => {
            match column.cast(&DataType::String) {
                //extract str type from already cast to string column
                Ok(some) => some,
                Err(e) => {
                    return Err(format!("Could not cast colum {} in an attempt to get a string column in the clean_amount_string method: {}", column.name(), e));
                }
            } 
        }
    };

    //get underlying string represented data
    let column_str = match column.str() {
        Ok(some) => some,
        Err(e) => {
            return Err(format!("Could not extract already converted string value as ChunkedArray from Series in clean_amount_string method: {}", e));
        }
    };

    //Get rid of second space and dolar sign
    //Only keep first 10 slices of string
    let column_str = column_str
        .apply(|chunkedarray|
            {
                match chunkedarray {
                    Some(val) => {
                        let cleaned_val = val
                            .chars()
                            .filter(|c| {
                                c.is_numeric() || 
                                c == &'.' ||
                                c == &'-'
                            })
                            .collect::<String>();
                        Some(Cow::from(cleaned_val))
                    },
                    None => None 
                }    
            }
       )
        .into_series();

    return Ok(column_str);
    //If first character is +, get rid of it
}
//TODO if directory does not exist, create it
//TODO get rid of unwraps

//IDEA for database
//csv storing transaction ids
//loads it into an in memory hash set AFTER reading new files, with capacity (existing + new dataset values)
//update hash set with newly written out values AFTER new file outputed
//save hashset back to csv file 

//TODO clean up all error messages