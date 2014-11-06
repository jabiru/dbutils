#-------------------------------------------------------------------------------
#
# Package dbutils 
#
# Wrappers for RODBC and JDBC. The main reason for JDBC is that 
# there's no longer a Netezza ODBC driver for Mac.  
# 
# Sergei Izrailev, 2012, 2014
#-------------------------------------------------------------------------------

# On Maverics (OS X 10.9) the .odbc.ini that matters for odbcinst must be in 
# in the home directory. However, the /Library/ODBC/odbc.ini is used to actually
# connect. Possibly it can be fixed with some environment variables. 
# unixODBC 2.3.1 is installed.

#' A set of utilities for connecting to relational databases using ODBC and JDBC.
#' 
#' The default behavior is to use ODBC, but in some cases, for example for 
#' Netezza on Mac, there's no ODBC driver (any more), so one has to use JDBC. 
#' In order to keep the choice of ODBC vs JDBC transparent to the user, the 
#' functions in \code{dbutils} rely on \code{options()} to differentiate between 
#' the two (see Examples below). The JDBC code leverages ODBC DSN setup to 
#' construct connection information and avoid the necessity to duplicate that
#' information elsewhere. As a consequence, the \code{Port} field must be added 
#' to all DSNs that will be used for JDBC connections.
#' 
#' The option 'DB.DEFAULT.DBTYPE' should be set to the string that appears
#' in the JDBC url after 'jdbc:', i.e., in 'jdbc:<dbtype>://<server>:<port>/database'.
#' Possible values are 'netezza', 'mysql', etc. 
#' 
#' The option 'JDBC.DRIVERS.INFO' should be set to a list of settings for each
#' DBTYPE, defining driver.class, driver.path and identifier.quote.
#' 
#' \code{db.check.dsn} - checks if the provided DSN is defined. 
#' 
#' @examples
#' \dontrun{
#' # To set up the environment, add the following to the ~/.Rprofile file:
#' options(
#'    JDBC.DRIVERS.INFO = list(
#'       netezza = list(
#'          driver.class = "org.netezza.Driver"
#'          , driver.path = "/Library/ODBC/nzjdbc.jar"
#'          , identifier.quote = "`"
#'       )
#'    ),
#'    DB.DEFAULT.CONNTYPE = "JDBC",
#'    DB.DEFAULT.DBTYPE = "netezza",
#'    DB.DSN.EXTRAS = list(
#'         mysql_hs = list(dbtype = "mysql", conntype = "JDBC")
#'       , myodbc = list(dbtype = "mysql", conntype = "JDBC")
#'    )
#' )
#' 
#' #*** Add the Port=5840 field to the DSN definitions if it's not there yet.
#' 
#' # The .tgz package for Mac may be compiled on Mavericks, whereas it needs to 
#' # run on Mountain Lion. In this case, the following commands fix the library paths: 
#' sudo ln -s /opt/local/lib/gcc47/libgomp.1.dylib /opt/local/lib/libgcc/libgomp.1.dylib
#' sudo ln -s /opt/local/lib/gcc47/libstdc++.6.dylib /opt/local/lib/libgcc/libstdc++.6.dylib
#' }
#' 
#' @name dbutils
#' @aliases dbutils
#' @title ODBC and JDBC wrappers
#' @rdname dbutils
db.check.dsn <- function(dsn, stop.if.bad = TRUE)
{   
   require("RODBC")
   if (!is.character(dsn)) stop("DSN is invalid. Expecting a string.")
   
   dsn.exists <- dsn %in% names(odbcDataSources())
   if (!dsn.exists && stop.if.bad)
   {
      stop(paste("DSN '", dsn, "' is not defined", sep = ""))      
   }
}

#-------------------------------------------------------------------------------

db.supported.dbtypes <- function() { c("netezza", "mysql") }

#-------------------------------------------------------------------------------

db.get.dbtype <- function(dsn)
{
   dbtype.default = tmpl.get.option("DB.DEFAULT.DBTYPE", default = "netezza")
   dsn.extras <- options()$DB.DSN.EXTRAS
   if (is.null(dsn.extras)) return(dbtype.default)
   if (!(dsn %in% names(dsn.extras))) return(dbtype.default)
   dbtype <- dsn.extras[[dsn]]$dbtype
   if (is.null(dbtype)) return(dbtype.default)
   if (!(dbtype %in% db.supported.dbtypes()))
   {
      stop(paste("Unsupported dbtype:", dbtype, "\nSupported dbtypes:\n", 
                  paste(tmpl.quote.csv(db.supported.dbtypes())), sep = ""))
   }
   return(dbtype)      
}

#-------------------------------------------------------------------------------

db.get.conntype <- function(dsn)
{
   conntype.default = tmpl.get.option("DB.DEFAULT.CONNTYPE", default = "ODBC")
   dsn.extras <- options()$DB.DSN.EXTRAS
   if (is.null(dsn.extras)) return(conntype.default)
   if (!(dsn %in% names(dsn.extras))) return(conntype.default)
   conntype <- dsn.extras[[dsn]]$conntype
   if (is.null(conntype)) return(conntype.default)
   if (!(toupper(conntype) %in% c("ODBC", "JDBC")))
   {
      stop(paste("Unsupported connection type:", conntype, 
                  "\nOnly ODBC and JDBC are supported.\n", sep = ""))
   }
   return(conntype)      
}

#-------------------------------------------------------------------------------

#' \code{db.connect} - connects to the database via ODBC or JDBC using the specified DSN.
#' @rdname dbutils
db.connect <- function(dsn
   , conn.type = db.get.conntype(dsn)
   , dbtype = db.get.dbtype(dsn)
   , password = NULL) 
{  
   invisible(switch(toupper(conn.type),
      ODBC = db.connect.odbc(dsn)
      , JDBC = db.connect.jdbc(dsn, dbtype, password)
   ))
}

#-------------------------------------------------------------------------------

#' \code{db.connect.odbc} - connects to the database via ODBC using the specified DSN.
#' @rdname dbutils
db.connect.odbc <- function(dsn) 
{
   require("RODBC")
   db.check.dsn(dsn)
   assign(".dbutilsConnection", odbcConnect(dsn), envir = globalenv())
   assign(".dbutilsConnType", "ODBC", envir = globalenv())
   invisible(as.logical(.dbutilsConnection))   
}

#-------------------------------------------------------------------------------

#' \code{db.connect.jdbc} - opens a JDBC connection to the database specified in DSN.
#' 
#' @rdname dbutils
db.connect.jdbc <- function(dsn, dbtype, password = NULL) 
{
   require("RJDBC")
   
   # Driver info should be set up in .Rprofile as
   #options(JDBC.DRIVERS.INFO = list(
   #   netezza = list(
   #        driver.class = "org.netezza.Driver"
   #      , driver.path = "/Library/ODBC/nzjdbc.jar"
   #      , identifier.quote = "`"
   #      )
   #   )
   #)
   
   # Check for presence of the driver info.
   driver.info <- options()$JDBC.DRIVERS.INFO[[dbtype]]
   if (length(driver.info) == 0) 
   {
      stop(paste("Unspecified driver for platform:", dbtype, "- check R options for JDBC.DRIVERS.INFO"))      
   } 
   
   # Get necessary fields from DSN.
   attributes <- c("DSN", "Servername", "Username", "Database", "Port", "Password")
   dsn.attributes <- db.dsn.info(dsn)
   
   if (!is.null(password))
   {
      # Use the provided password.
      dsn.attributes$Password <- password
   } 
   else 
   {
      # Use the password from the DSN. This requires dsninst or a a working ODBC connection.      
      # If we used a real ODBC connection to get there, the password is replaced with '*'s 
      # so this method won't work. Besides, then there's no point to use JDBC anyway.
      grep.res <- grepl("[^\\*]", dsn.attributes$Password)
      if (length(grep.res) == 0) 
      {
         stop("db.connect.jdbc: No 'Password' field in dsn attributes.")
      }
      if (grep.res[1] == FALSE) 
      {
         stop("db.connect.jdbc: 'Password' field in dsn attributes is masked. Check for working odbcinst.")
      }
   }
   
   # Check if all information is available
   missing.attr <- setdiff(attributes, names(dsn.attributes))
   if (length(missing.attr) > 0) 
   {
      stop(paste("db.connect.jdbc: non-existing attributes (case sensitive):", missing.attr))
   }
   
   # Construct the connection.
   conn.url <- paste("jdbc:", dbtype, "://", dsn.attributes$Servername, ":", dsn.attributes$Port, "/", dsn.attributes$Database, sep = "")
   drv <- JDBC(driver.info$driver.class, driver.info$driver.path, identifier.quote = driver.info$identifier.quote)
   conn <- dbConnect(drv, conn.url, user = dsn.attributes$Username, password = dsn.attributes$Password)
   
   #drv <- JDBC("org.netezza.Driver", "/Users/Sergei/Desktop/Netezza/nzjdbc.jar", identifier.quote="`")
   #conn <- dbConnect(drv, "jdbc:netezza://ernie:5480/datasci", user="sizrailev", password = "")
   
   assign(".dbutilsConnection", conn, envir = globalenv())
   assign(".dbutilsConnType", "JDBC", envir = globalenv())
   invisible(is.null(.dbutilsConnection))   
}

#-------------------------------------------------------------------------------

#' \code{db.odbc.connection.string} - extracts ODBC connection string by establishing and closing a connection.
#' 
#' @rdname dbutils
db.odbc.connection.string <- function(dsn)
{
   require("RODBC")
   odbc.connection <- odbcConnect(dsn)
   if (class(odbc.connection) != "RODBC") stop(paste("db.odbc.get.connection.string: Can't connect to DSN:", dsn))
   connection.str = attr(odbc.connection, "connection.string")
   odbcClose(odbc.connection)
   return(connection.str)
}

#-------------------------------------------------------------------------------

#' \code{db.dsn.info} returns DSN information as a named list.
#' 
#' @rdname dbutils
db.dsn.info <- function(dsn)  
{
   conn.type = db.get.conntype(dsn)

   # Check if DSN is defined
   if (conn.type == "ODBC") db.check.dsn(dsn)

   # Check if odbcinst is present
   # TODO: on Windows, there's probably no odbcinst at all; find out alternative source of info
   odbcinst <- tryCatch(system("which odbcinst", intern=T), error=function(e) c("ERROR", e$message), warning=function(e) c("WARNING", e$message))
   if (odbcinst[1] == "ERROR" || odbcinst[1] == "WARNING")
   {
      if (conn.type == "ODBC")
      {         
         # For backward compatibility, only issue a warning and try to get info from connection.
         # This method doesn't retrieve the password though, so jdbc connections won't work.     
         warning(paste("db.dsn.info: Can't find odbcinst in the path:", odbcinst[1], odbcinst[2], 
                     ". Attempting to connect."))
         connection.str <- db.odbc.connection.string(dsn)
         split.connection.str = unlist(strsplit(connection.str,';'))
      }
      else
      {
         stop(paste("db.dsn.info: Can't find odbcinst in the path:", odbcinst[1], odbcinst[2]))                     
      }
   } 
   else 
   {
      odbcinst <- paste(odbcinst, "-q -s -n")
      dsn.info <- run.remote(paste(odbcinst, dsn), remote = "")
      if (dsn.info$cmd.error)
      {
         if (conn.type == "ODBC")
         {
            warning(paste("db.dsn.info: Error running odbcinst.", 
                        paste(dsn.info$cmd.out, collapse="\n"), dsn.info$warn.msg, 
                        "Attempting to connect.", sep="\n"))
            connection.str <- db.odbc.connection.string(dsn)            
            split.connection.str = unlist(strsplit(connection.str,';'))         
         }
         else
         {
            stop(paste("db.dsn.info: Error running odbcinst.", 
                        paste(dsn.info$cmd.out, collapse="\n"), dsn.info$warn.msg, sep="\n"))            
         }
      } 
      else 
      {
         split.connection.str <- c(paste("DSN=", dsn, sep = ""), dsn.info$cmd.out[-1])         
      }
   }
   
   split.attr.values = lapply(split.connection.str, FUN = function(x) { y = unlist(strsplit(x,'=')); return(y[2])}) 
   split.attr.names = sapply(split.connection.str, FUN = function(x) { y = unlist(strsplit(x,'=')); return(y[1])}) 
   names(split.attr.values) = split.attr.names
   # In ODBC connection string, username is called 'UID', so rename it back to be consistent.
   names(split.attr.values)[match("UID", names(split.attr.values))] <- "Username"
   names(split.attr.values)[match("PWD", names(split.attr.values))] <- "Password"
   return(as.data.frame(split.attr.values, stringsAsFactors = FALSE))
}

#-------------------------------------------------------------------------------

#' \code{db.connected} returns TRUE if there is a connection initiated 
#' through \code{db.connect}.
#' @rdname dbutils
db.connected <- function() 
{
   if (!exists(".dbutilsConnection") || is.null(.dbutilsConnection)) return(FALSE)
   if (inherits(.dbutilsConnection, "RODBC") || inherits(.dbutilsConnection, "JDBCConnection"))
   {
      return(TRUE)
   }
}

#-------------------------------------------------------------------------------

#' \code{db.disconnect} closes the connection made with \code{db.connect}
#' @rdname dbutils
db.disconnect <- function()
{   
   if (db.connected()) 
   {
      switch(.dbutilsConnType,
         ODBC = { require(RODBC); try(odbcClose(.dbutilsConnection), silent = TRUE) },            
         JDBC = { require(RJDBC); try(dbDisconnect(.dbutilsConnection), silent = TRUE) }
      )
      if (exists(".dbutilsConnection"))
      {
         rm(".dbutilsConnection", envir = globalenv())         
      }
      if (exists(".dbutilsConnType"))
      {
         rm(".dbutilsConnType", envir = globalenv())         
      }
   }   
}

#-------------------------------------------------------------------------------

#' \code{db.query} runs a query using the connection open with \code{db.connect}.
#' @rdname dbutils
db.query <- function (..., as.is = FALSE) 
{
   if (!db.connected()) stop("connection not opened", call. = FALSE)
   
   query <- paste(..., sep = "", collapse = "")
   result <- switch(.dbutilsConnType,
      ODBC = db.query.odbc(query, as.is = as.is),
      JDBC = db.query.jdbc(query)
   )
   return(result)
}

#-------------------------------------------------------------------------------

#' \code{db.query.odbc} runs a query using the ODBC connection open with \code{db.connect}.
#' @rdname dbutils
db.query.odbc <- function (..., as.is = FALSE) 
{
   require("RODBC")
   if (!db.connected()) stop("connection not opened", call. = FALSE)
   
   query <- paste(..., sep = "", collapse = "")
   result <- sqlQuery(.dbutilsConnection, query, believeNRows = FALSE, stringsAsFactors = FALSE, as.is = as.is)
   if (is.character(result) && length(result) > 0) 
   {
      stop(paste(result, collapse = "\n"))
   }
   return(result)
}

#-------------------------------------------------------------------------------

#' \code{db.query.jdbc} runs a query using the JDBC connection open with \code{db.connect}.
#' @rdname dbutils
db.query.jdbc <- function (...) 
{
   require("RJDBC")
   if (!db.connected()) stop("connection not opened", call. = FALSE)
   
   query <- paste(..., sep = "", collapse = "")
   # JDBC has a special function for DML queries (dbSendUpdate), but we don't know
   # whether or not a query is a DML query, so instead we catch the error messages 
   # complaining about the empty result set and pretend it's OK. This way, the behavior
   # is exactly the same as for db.query.odbc.
   result <- tryCatch(dbGetQuery(.dbutilsConnection, query), error=function(e) c("ERROR", e$message), warning=function(e) c("WARNING", e$message))   
   if (is.character(result) && length(result) > 0) 
   {
      # This is really hacky: if the error message changes, this will stop working.
      # For Netezza driver
      if (result[1] == "ERROR" && grepl("(No results were returned by the query.)", result[2]))
      {
         # Query executes, but there's an error because no results are returned.
         return(as.character(c()))
      }
      # For MySQL driver
      else if (result[1] == "ERROR" && grepl("(Can not issue data manipulation statements with executeQuery().)", result[2]))
      {
         # Query doesn't run to begin with, so do the same with a DML version.
         dbSendUpdate(.dbutilsConnection, query)
         return(as.character(c()))
      }
      stop(paste(result, collapse = "\n"))
   }
   return(result)
}

#-------------------------------------------------------------------------------

#' \code{db.dsn.attr} returns a named list of DSN properties with the given attributes.
#' @rdname dbutils
db.dsn.attr <- function(dsn, attributes = c("DSN", "Servername", "Username", "Database")) 
{
   dsn.attributes <- db.dsn.info(dsn)
   missing.attr <- setdiff(attributes,names(dsn.attributes))
   if (length(missing.attr) > 0) stop(paste("non-existing attributes (case sensitive):", missing.attr))
   return (dsn.attributes[names(dsn.attributes) %in% attributes])
}

#-------------------------------------------------------------------------------

