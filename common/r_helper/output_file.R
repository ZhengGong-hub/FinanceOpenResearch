#' Save Regression Results to CSV File
#' 
#' This function saves regression model results to a CSV file. If the file already exists,
#' the new results will be appended to the existing data. Otherwise, a new file will be created.
#' 
#' @param results_df A data frame containing regression results, typically with columns for
#'        variable names, coefficients, standard errors, t-values, and p-values.
#' @param output_file The file path where the CSV file should be saved.
#' 
#' @return None. The function writes the results to the specified file.
#' 
#' @examples
#' # Create a sample results data frame
#' results <- data.frame(
#'   Variable = c("intercept", "x1"),
#'   Coefficient = c(1.2, 0.5),
#'   Std_Error = c(0.1, 0.05),
#'   t_value = c(12, 10),
#'   p_value = c(0.001, 0.002)
#' )
#' # Save to a file
#' save_results_to_csv(results, "model_results.csv")
save_results_to_csv <- function(results_df, output_file, append = TRUE) {
    # Check if file exists and append if it does
    if (file.exists(output_file) && append) {
    # Read existing data
    existing_data <- read.csv(output_file)
    # Append new data
    combined_data <- rbind(existing_data, results_df)
    write.csv(combined_data, output_file, row.names = FALSE)
    } else {
    # Create new file if it doesn't exist
    write.csv(results_df, output_file, row.names = FALSE)
    }
}

#' Extract Regression Results from a fixest Model
#' 
#' This function extracts key statistics from a fixest model object and returns them
#' as a data frame. The extracted statistics include variable names, coefficients,
#' standard errors, t-statistics, and p-values.
#' 
#' @param model A model object created by the feols function from the fixest package.
#' 
#' @return A data frame containing the regression results with columns for variable names,
#'         coefficients, standard errors, t-values, and p-values.
#' 
#' @examples
#' # Assuming you have a fixest model:
#' # model <- feols(y ~ x1 + x2 | fe1 + fe2, data = my_data)
#' # results_df <- save_feols_to_df(model)
save_feols_to_df <- function(model) {
    data.frame(
        Variable = names(coef(model)),
        Coefficient = coef(model),
        Std_Error = se(model),
        t_value = tstat(model),
        p_value = pvalue(model)
    )
}
