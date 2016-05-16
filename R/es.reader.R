#' Read an ElasticSearch index described in a .es file.
#'
#' This function will load data from an ElasticSearch index based on configuration
#' information found in the specified .es file. The .es file must specify
#' an ElasticSearch sercer to be accessed. A specific query against any index may be executed to generate
#' a data set.
#'
#' Example of the DCF format and settings used in a .es file are shown
#' below:
#'
#' host: localhost
#' port: 9200
#' index: sample_index
#' field: text
#' query: this AND that
#'
#' @param data.file The name of the data file to be read.
#' @param filename The path to the data set to be loaded.
#' @param variable.name The name to be assigned to in the global environment.
#'
#' @return No value is returned; this function is called for its side effects.
#'
#' @importFrom ProjectTemplate translate.dcf
#'
#' @examples
#' library('ProjectTemplate')
#'
#' \dontrun{es.reader('example.es', 'data/example.es', 'example')}
es.reader <- function(data.file, filename, variable.name)
{
  server.info <- translate.dcf(filename)

  # Default value for 'port' in RElasticSearch is 9200L.
  if (is.null(server.info[['port']]))
  {
    server.info[['port']] <- 9200
  }
  
  elastic::connect(es_base = server.info[['host']], es_port = as.integer(server.info[['port']]))
  
  index <- server.info[['index']]

  query <- server.info[['query']]
  field <- server.info[['field']]
  
  if (is.null(query))
  {
    warning("'query' must be specified in a .es file")
    return()
  }
  
  if (is.null(field))
  {
    warning("'field' must be specified in a .es file")
    return()
  }

  if (! is.null(query) && ! is.null(field))
  {
    if (length(grep('\\{\\{.*\\}\\}', query))) {
      require.package('whisker')
      query <- whisker.render(query, data = .GlobalEnv)       
    }
    
    match <- list(query = list(
      bool = list(
        must = list(
          query_string = list(
            default_field = field,
            query = query
          )
        )
      )
    ))
    
    data.parcel <- try({
      ## size = elastic::Search(index = index, body = match, size = 0)$hits$total 
      ## elastic::Search(index = index, body = match, size = size) 
      res <- Search(index = index, body = match, scroll="5m", search_type = "scan")
      out <- list()
      hits <- 1
      while(hits != 0){
        res <- scroll(scroll_id = res$`_scroll_id`)
        hits <- length(res$hits$hits)
        if(hits > 0)
          out <- c(out, res$hits$hits)
      }
      out
    })
                       
                       if (class(data.parcel) == 'AsIs') 
                       {
                         warning(paste(query, ' returned empty result.', sep=''))
                         return()
                       }

                       if (class(data.parcel) == 'list')
                       {
                         
                         # structure is a list of lists
                         # Convert to DataFrame
                         
                         # Store names for later
                         
                         data.parcel <- plyr::ldply(data.parcel, as.data.frame)
                         
                         names = names(data.parcel)
                         names = gsub('^X_', '', names)
                         
                         names(data.parcel) = names
                         
                         assign(variable.name,
                                data.parcel,
                                envir = .GlobalEnv)
                       }
                       
                       else
                       {
                         warning(paste("Error loading '",
                                       variable.name,
                                       "' with query '",
                                       query, "'.",
                                       sep = ''))
                         return()
                       }
    }

    # If the table exists but is empty, do not create a variable.
    # Or if the query returned no results, do not create a variable.
    if (nrow(data.parcel) == 0)
    {
      assign(variable.name,
             NULL,
             envir = .GlobalEnv)
      return()
    }

  }

  .onLoad <- function(...)
  {
    .add.extension('es', es.reader)
  }



