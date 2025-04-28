# R script for regression analysis using DuckDB
library(fixest)
library(dplyr)
library(duckdb)
library(modelsummary)

# Print a message to confirm the script is running
print("Starting regression analysis with DuckDB")

# Check if db_path and table_name are provided as command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 2) {
  db_path <- args[1]
  table_name <- args[2]
  output_dir <- args[3]
  print(paste("Using database path:", db_path))
  print(paste("Using table name:", table_name))
  print(paste("Using output path:", output_dir))
} else {
  stop("Database path and table name not provided. Please provide the path to the DuckDB database and the table name as command line arguments.")
}
# Connect to the DuckDB database and run analysis with error handling
tryCatch({
  # Connect to the DuckDB database
  con <- dbConnect(duckdb::duckdb(), db_path)
  print("Successfully connected to DuckDB database")
  
  # Load the data from the DuckDB database
  data <- dbGetQuery(con, paste0("SELECT * FROM ", table_name))
  print(paste("Successfully loaded data with", nrow(data), "rows and", ncol(data), "columns"))
  
  # Print the first 5 rows of the data
  print("First 5 rows of the data:")
  # print(head(data, 5))
  
  # Run regression models
  print("Running regression models...")
  
  # Model 1: Overall ESG
  print("Model 1: Overall ESG")
  model1 <- feols(recs_panel_rec_code ~ msci_esg_WEIGHTED_SCORE + esg_esg_pctg + hetero_esg
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model1_summary <- summary(model1)
  print("Model 1 completed")
  
  # Model 2: Environmental Pillar
  print("Model 2: Environmental Pillar")
  model2 <- feols(recs_panel_rec_code ~ msci_esg_ENVIRONMENTAL_PILLAR_SCORE + esg_e_pctg + hetero_esg_e
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model2_summary <- summary(model2)
  print("Model 2 completed")
  
  # Model 3: Social Pillar
  print("Model 3: Social Pillar")
  model3 <- feols(recs_panel_rec_code ~ msci_esg_SOCIAL_PILLAR_SCORE + esg_s_pctg + hetero_esg_s
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model3_summary <- summary(model3)
  print("Model 3 completed")
  
  # Model 4: Governance Pillar
  print("Model 4: Governance Pillar")
  model4 <- feols(recs_panel_rec_code ~ msci_esg_GOVERNANCE_PILLAR_SCORE + esg_g_pctg + hetero_esg_g
                  + 1 | isin + recs_panel_beg_date + recs_panel_AMASKCD, data = data)
  model4_summary <- summary(model4)
  print("Model 4 completed")
  
  # Create output directory if it doesn't exist
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  print(paste("Output directory created:", output_dir))
  
  # Export regression results to CSV
  models_list <- list(
    "Overall ESG" = model1,
    "Environmental" = model2,
    "Social" = model3,
    "Governance" = model4
  )
  
  # Export individual model results
  for (i in 1:length(models_list)) {
    model_name <- names(models_list)[i]
    model <- models_list[[i]]
    
    # Get coefficients and statistics
    coefs <- coef(model)
    se <- se(model)
    tstat <- coefs / se
    pval <- 2 * pt(abs(tstat), df = model$nobs - length(coefs) - 1, lower.tail = FALSE)
    
    # Create data frame with results
    results_df <- data.frame(
      Variable = names(coefs),
      Coefficient = coefs,
      Std_Error = se,
      t_value = tstat,
      p_value = pval
    )
    
    # Write to CSV
    output_file <- file.path(output_dir, paste0("model_", i, "_", gsub(" ", "_", model_name), ".csv"))
    write.csv(results_df, output_file, row.names = FALSE)
    print(paste("Results for Model", i, "saved to", output_file))
  }
  
  # Export combined model results
  modelsummary(models_list, 
               output = file.path(output_dir, "all_models.csv"),
               fmt = "%.4f",
               stars = TRUE,
               gof_omit = "IC|Log|F|R2 Within|R2 P")
  
  print("Analysis completed successfully")
  
}, error = function(e) {
  print(paste("ERROR: An error occurred during analysis:", e$message))
  stop(e)
}, finally = {
  # Close the DuckDB connection if it exists
  if(exists("con") && inherits(con, "duckdb_connection")) {
    dbDisconnect(con, shutdown=TRUE)
    print("DuckDB connection closed")
  }
})
