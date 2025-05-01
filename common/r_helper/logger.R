#' Log Message with Timestamp in JSON Format
#' 
#' This function logs a message with a timestamp in JSON format to the console.
#' It ensures that the message is immediately displayed by flushing the console
#' after writing the message.
#' 
#' @param msg A character string containing the message to be logged.
#' 
#' @return None. The function outputs the formatted message to the console.
#' 
#' @examples
#' # Log a simple message
#' log_message("Starting data processing")
#' 
#' # Log a message with dynamic content
#' log_message(paste("Processed", nrow(data), "rows of data"))
log_message <- function(msg) {
  timestamp <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS6")
  json_log <- paste0('{"source": "Rscript", "timestamp": "', timestamp, '", "message": "', msg, '"}')
  cat(paste0(json_log, "\n"))
  flush.console()
}