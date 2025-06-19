# R script for regression analysis using DuckDB (MSCI data)
library(fixest)
library(dplyr)
library(duckdb)
library(modelsummary)
source("common/r_helper/output_file.R")
source("common/r_helper/logger.R")

# Print a message to confirm the script is running
log_message("Starting regression analysis with DuckDB (MSCI)")

# Check if db_path and table_name are provided as command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 3) {
  db_path <- args[1]
  table_name <- args[2]
  output_dir <- args[3]
  log_message(paste("Using database path:", db_path))
  log_message(paste("Using table name:", table_name))
  log_message(paste("Using output path:", output_dir))
} else {
  stop("Database path, table name, and output directory must be provided as command line arguments.")
}

# Connect to the DuckDB database and run analysis with error handling
# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_path)
log_message("Successfully connected to DuckDB database")

# Load the data from the DuckDB database
data <- dbGetQuery(con, paste0("SELECT * FROM ", table_name))
log_message(paste("Successfully loaded data with", nrow(data), "rows and", ncol(data), "columns"))

# preprocess data
data$hetero_esg   <- data$esg_esg_pctg * data$msci_esg_WEIGHTED_SCORE
data$hetero_esg_e <- data$esg_e_pctg   * data$msci_esg_ENVIRONMENTAL_PILLAR_SCORE
data$hetero_esg_s <- data$esg_s_pctg   * data$msci_esg_SOCIAL_PILLAR_SCORE
data$hetero_esg_g <- data$esg_g_pctg   * data$msci_esg_GOVERNANCE_PILLAR_SCORE

# esg_rolling_num_q > 10
data <- data %>%
  filter(esg_rolling_num_q >= 10)

# Create output directory if it doesn't exist
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
log_message(paste("Output directory created:", output_dir))

# Define cutoff years to loop through
cutoff_year <- 2012

# Define model configurations
model_configs <- list(
  list(name = "Overall_ESG", var = "hetero_esg", score = "msci_esg_WEIGHTED_SCORE"),
  list(name = "Environmental", var = "hetero_esg_e", score = "msci_esg_ENVIRONMENTAL_PILLAR_SCORE"),
  list(name = "Social", var = "hetero_esg_s", score = "msci_esg_SOCIAL_PILLAR_SCORE"),
  list(name = "Governance", var = "hetero_esg_g", score = "msci_esg_GOVERNANCE_PILLAR_SCORE")
)

# Function to run a model and save results
run_and_save_model <- function(var_name, model_name, data, cutoff_year) {
  log_message(paste("Model:", model_name, "- Cutoff year", cutoff_year))
  
  # Run the model with both individual terms and interaction
  formula <- as.formula(paste("recs_panel_rec_code ~", var_name,
                            "| isin^recs_panel_beg_date + recs_panel_AMASKCD^recs_panel_beg_date"))
  model <- feols(formula, data = data)
  log_message(paste("Model completed for", model_name, "with cutoff year", cutoff_year))
  
  # print results
  print(summary(model))
}


# Filter data based on current cutoff year
filtered_data <- data[data$recs_panel_year >= cutoff_year, ]

# Run regression models
log_message(paste("Running regression models for cutoff year", cutoff_year, "..."))

# Run all models for this cutoff year
for (config in model_configs) {
  run_and_save_model(
    var_name = config$var,
    model_name = config$name,
    data = filtered_data,
    cutoff_year = cutoff_year
  )
}

# Close the database connection
dbDisconnect(con, shutdown=TRUE)
