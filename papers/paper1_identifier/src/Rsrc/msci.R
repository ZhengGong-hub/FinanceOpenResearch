# R script for regression analysis using DuckDB
library(fixest)
library(dplyr)
library(duckdb)

# Print a message to confirm the script is running
print("Starting regression analysis with DuckDB")

# Check if db_path and table_name are provided as command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 2) {
  db_path <- args[1]
  table_name <- args[2]
  print(paste("Using database path:", db_path))
  print(paste("Using table name:", table_name))
} else {
  stop("Database path and table name not provided. Please provide the path to the DuckDB database and the table name as command line arguments.")
}

# Connect to the DuckDB database
tryCatch({
  con <- dbConnect(duckdb::duckdb(), db_path)
  print("Successfully connected to DuckDB database")
  
  # Load the data from the DuckDB database
  data <- dbGetQuery(con, paste0("SELECT * FROM ", table_name))
  print(paste("Successfully loaded data with", nrow(data), "rows and", ncol(data), "columns"))
  
  # Print the first 5 rows of the data
  print("First 5 rows of the data:")
  print(head(data, 5))
  
  # Run regression models
  print("Running regression models...")
  
  # Model 1: Overall ESG
  print("Model 1: Overall ESG")
  model1 <- feols(recs_panel_rec_code ~ msci_esg_WEIGHTED_SCORE + esg_esg_pctg + hetero_esg
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model1_summary <- summary(model1)
  print(model1_summary)
  
  # Model 2: Environmental Pillar
  print("Model 2: Environmental Pillar")
  model2 <- feols(recs_panel_rec_code ~ msci_esg_ENVIRONMENTAL_PILLAR_SCORE + esg_e_pctg + hetero_esg_e
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model2_summary <- summary(model2)
  print(model2_summary)
  
  # Model 3: Social Pillar
  print("Model 3: Social Pillar")
  model3 <- feols(recs_panel_rec_code ~ msci_esg_SOCIAL_PILLAR_SCORE + esg_s_pctg + hetero_esg_s
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model3_summary <- summary(model3)
  print(model3_summary)
  
  # Model 4: Governance Pillar
  print("Model 4: Governance Pillar")
  model4 <- feols(recs_panel_rec_code ~ msci_esg_GOVERNANCE_PILLAR_SCORE + esg_g_pctg + hetero_esg_g
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model4_summary <- summary(model4)
  print(model4_summary)
  
  print("Analysis completed successfully")
}, error = function(e) {
  print(paste("Error in R script:", e$message))
  quit(status = 1)
}, finally = {
  # Close the DuckDB connection
  if (exists("con")) {
    dbDisconnect(con, shutdown=TRUE)
    print("DuckDB connection closed")
  }
})

