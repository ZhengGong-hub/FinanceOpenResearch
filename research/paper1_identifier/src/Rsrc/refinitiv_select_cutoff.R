# R script for regression analysis using DuckDB (Refinitiv data)
library(fixest)
library(dplyr)
library(duckdb)
library(modelsummary)
source("common/r_helper/output_file.R")
source("common/r_helper/logger.R")

# Print a message to confirm the script is running
log_message("Starting regression analysis with DuckDB (Refinitiv)")

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
data$hetero_esg   <- data$esg_esg_pctg * data$Scores_ESGCombinedScore
data$hetero_esg_e <- data$esg_e_pctg   * data$Scores_EnvironmentPillarScore
data$hetero_esg_s <- data$esg_s_pctg   * data$Scores_SocialPillarScore
data$hetero_esg_g <- data$esg_g_pctg   * data$Scores_GovernancePillarScore

# esg_rolling_num_q > 10
data <- data %>%
  filter(esg_rolling_num_q >= 10)

# Create output directory if it doesn't exist
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
log_message(paste("Output directory created:", output_dir))

# Define cutoff years to loop through
cutoff_years <- c(2008, 2010, 2012, 2014, 2016, 2018)

# Define model configurations
model_configs <- list(
  list(name = "Overall_ESG", var = "hetero_esg"),
  list(name = "Environmental", var = "hetero_esg_e"),
  list(name = "Social", var = "hetero_esg_s"),
  list(name = "Governance", var = "hetero_esg_g")
)

# Function to run a model and save results
run_and_save_model <- function(var_name, model_name, data, cutoff_year, is_first_cutoff) {
  log_message(paste("Model:", model_name, "- Cutoff year", cutoff_year))
  
  # Run the model
  formula <- as.formula(paste("recs_panel_rec_code ~", var_name, 
                              "| isin^recs_panel_beg_date + recs_panel_AMASKCD^recs_panel_beg_date"))
  model <- feols(formula, data = data)
  log_message(paste("Model completed for", model_name, "with cutoff year", cutoff_year))
  
  # Save results
  reg_results_df <- save_feols_to_df(model)
  # add cutoff year to the results
  reg_results_df$cut_off_year <- cutoff_year
  
  output_file <- file.path(output_dir, paste0("model_", gsub(" ", "_", model_name), ".csv"))
  save_results_to_csv(reg_results_df, output_file, append = !is_first_cutoff)
  log_message(paste("Results for", model_name, "for cutoff year", 
                    cutoff_year, "saved to", output_file))
  
  return(model)
}

# Loop through each cutoff year
for (cutoff_year in cutoff_years) {  
  # Filter data based on current cutoff year
  filtered_data <- data[data$recs_panel_year >= cutoff_year, ]
  
  # Run regression models
  log_message(paste("Running regression models for cutoff year", cutoff_year, "..."))
  
  # Flag to check if this is the first cutoff year (for append parameter)
  is_first_cutoff <- cutoff_year == cutoff_years[1]
  
  # Run all models for this cutoff year
  for (config in model_configs) {
    model <- run_and_save_model(
      var_name = config$var,
      model_name = config$name,
      data = filtered_data,
      cutoff_year = cutoff_year,
      is_first_cutoff = is_first_cutoff
    )
  }
}

# Close the database connection
dbDisconnect(con, shutdown=TRUE)