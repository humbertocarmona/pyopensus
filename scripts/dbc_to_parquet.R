# -- install necessary packages if not already installed
if (!requireNamespace("read.dbc", quietly = TRUE)) install.packages("read.dbc")
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")

# -- load libraries
library(read.dbc)
library(arrow)

# -- parse command-line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  stop("Please provide both input and output file paths.")
}

input_file <- args[1]
output_file <- args[2]

# -- read .dbc file
data <- read.dbc::read.dbc(input_file)

# -- ensure all character columns are encoded in UTF-8
data[] <- lapply(data, function(col) {
  if (is.character(col)) return(enc2utf8(col))
  return(col)
})

# -- write to .parquet file
arrow::write_parquet(data, output_file)