#-------------------------------------------------------------------------------
#
# Package dbutils 
#
# Utilities specific to Netezza's ODBC query interface.
# 
# Sergei Izrailev, 2011-2014
#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------

#' \code{db.nzquery} runs the sql query using a DSN connection defined on the machine. The end state is DISCONNECTED.  
#' Uses the RODBC package and runs db.nzodbc.query(sql, as.is = FALSE). Connects and disconnects each time. 
#' If convert.names is TRUE, converts all "_" in the column names to "." and makes the column names lower case.
#' @param SQL The SQL query.
#' @param dsn The connection DSN passed to \code{\link{db.nzodbc.connect.dsn()}}. 
#'        Defaults to dsn = "NZSQL", but the user can specify a different default value by setting 
#'        \code{options("NZDSN")}. This way, the code doesn't need to know about a global default setting.
#' @param convert.names If \code{TRUE}, converts all "_" in the column names to "." and makes the column names lower case, 
#'        for example, "COOKIE_ID" becomes "cookie.id".
#' @param verbose If set to \code{TRUE}, prints the \code{SQL} before the query begins and the elapsed execution time after it ends. 
#' @param var.list If not NULL, variables in \code{SQL} are substituted with values defined in the \code{var.list}. 
#'        Use \code{db.cast()} to set proper data types. Note that substitution is recursive (left to right), 
#'        so you can use vars inside other vars, as long as the var order is correct.
#' @param col.types Optional list specifying the resulting column types. If a column name in the resulting 
#'        data frame matches a name in the col.types (case-insensitive), that column is converted to the specified type.
#'        The default is taken from the \code{NZCOL.TYPES} option.
#'        See \code{\link{db.convert.cols}} and \code{\link{db.nzset.coltypes}}.
#' @param all.literal If \code{TRUE}, all strings in \code{var.list} are treated as literals, i.e., 
#'        they won't be enclosed in quotes. This is useful when there are no strings that need to be 
#'        quoted. 
#' @examples 
#' \dontrun{
#' db.nzquery("select count(*) as cnt from mytab", dsn = "NZDSN")
#' #
#' # Example of using var.list:
#' query <- "select cookie_id, count(*) as cnt from MYTABLE 
#'           where date = THISDATE and run_id = THISRUNID and 
#'           type = THISSTRING group by cookie_id"
#' db.nzquery(query, var.list = list(THISDATE = as.Date('2012-04-01'), 
#'           THISSTRING = 'fnl', 
#'           MYTABLE = db.cast('this_table_THISRUNID', 'literal'), 
#'           THISRUNID = 32))
#' #
#' # Example of using col.types
#' ctypes <- list(uid = "int64", mmdef_uid = "int64", config_uid = "int64", 
#'    tab_meta = "literal")
#' frm <- db.nzquery("select * from mm_instance", dsn = "NZMM", col.types = ctypes)
#' unlist(lapply(frm, function(x) class(x)))
#'         UID   MMDEF_UID     TO_DATE  CONFIG_UID  MMDEF_DATE    TAB_META 
#'     "int64"     "int64"      "Date"     "int64"      "Date"   "literal" 
#'    TAB_BINS      TAB_MM  TAB_SAMPLE     COMMENT     NSLICES 
#' "character" "character" "character"   "logical"   "integer" 

#' }
#' @return \code{db.nzquery} - A data frame containing the results.
#' @name db.nzquery
#' @aliases db.nzquery
#' @title ODBC interface to Netezza
#' @seealso \code{\link{db.read.nz}}, \code{\link{db.varsub.sql}}
#' @rdname db.nzquery
# JMS: two changes: i) make var.list 2nd argument, 2) set verbose to default to TRUE 
db.nzquery <- function(SQL, var.list = NULL, dsn = db.get.nzdsn(caller = "db.nzquery"), 
      convert.names = FALSE, verbose = db.get.option("NZQUERY.VERBOSE", default = FALSE), 
      col.types = getOption("NZCOL.TYPES"), all.literal = FALSE, as.is = FALSE)
{
   if (db.db.connected()) db.db.disconnect()
   
   # Connect to the database (e.g., TF3)
   db.db.connect(dsn)
   
   if (!is.null(getOption("NZPRIORITY")))
   {
      if (tolower(getOption("NZPRIORITY")) == "high")
      {
         db.db.query("alter session set priority to high")
      }
   }
   
   # Substitute variables
   if (!is.null(var.list)) SQL <- db.varsub.sql(SQL, var.list, all.literal = all.literal)
   
   # Print query
   if (verbose) writeLines(paste("db.nzquery sql:", SQL))
   
   # Get data
   if (verbose) tic("db.nzquery") 
   frm <- db.db.query(SQL, as.is = as.is)
   if (verbose) toc()
   
   # Disconnect from the database
   db.db.disconnect()
      
   # Clean up names
   if (convert.names) names(frm) <- gsub("_", ".", tolower(names(frm)))
   
   # Return frm
   if (is.null(col.types)) return(frm)
   return(db.convert.cols(frm, col.types))      
}

#-------------------------------------------------------------------------------

#' \code{db.nzexec} - executes a vector of SQL commands using db.nzodbc.query(). 
#' @param sql.vec A vector of SQL statements to run.
#' @inheritParams db.nzquery
#' @return \code{db.nzexec} - A list of data frames with results for each query.
#' @rdname db.nzquery
db.nzexec <- function(sql.vec, var.list = NULL, dsn = db.get.nzdsn(caller = "db.nzexec"), 
      convert.names = FALSE, verbose = db.get.option("NZQUERY.VERBOSE", default = FALSE), 
      col.types = getOption("NZCOL.TYPES"), all.literal = FALSE, as.is = FALSE)
{
   if (db.db.connected()) db.db.disconnect()
   
   # Connect to the database (e.g., TF3)
   db.db.connect(dsn)
   
   if (!is.null(getOption("NZPRIORITY")))
   {
      if (tolower(getOption("NZPRIORITY")) == "high")
      {
         db.db.query("alter session set priority to high")
      }
   }
   
   # Substitute variables
   if (!is.null(var.list)) sql.vec <- db.varsub.sql(sql.vec, var.list, all.literal = all.literal)
   
   # Get data
   res <- lapply(sql.vec, 
         function(s) 
         { 
            # Print query
            if (verbose) writeLines(paste("db.nzexec sql:", s))
            
            # Run the query
            if (verbose) tic("db.nzexec")
            frm <- db.db.query(s, as.is = as.is)
            if (verbose) toc()
            
            # Clean up names
            if (convert.names) names(frm) <- gsub("_", ".", tolower(names(frm)))
            
            # Convert types and return
            if (!is.null(col.types)) return(db.convert.cols(frm, col.types)) 
            return(frm) 
         }) 
      
   # Disconnect from the database
   db.db.disconnect()
   
   return(res)
}

#-------------------------------------------------------------------------------

#' \code{db.get.nzdsn} - Helper function checking for the \code{NZDSN} option. If it 
#' doesn't exist, throws an error, otherwise returns the value. Primarily used as a
#' default \code{dsn} parameter in other functions.
#' @rdname db.nzquery
db.get.nzdsn <- function(caller = "") 
{
   if (is.null(getOption("NZDSN"))) 
   {
      stop(paste(caller, ": Couldn't find NZDSN in options, with no value provided as a parameter.", sep=""))
   }
   getOption("NZDSN")
}

#-------------------------------------------------------------------------------

#' \code{db.set.nzdsn} - Helper function setting the \code{NZDSN} option, which allows
#' many other functions to use it as the default DSN.
#' @rdname db.nzquery
db.set.nzdsn <- function(dsn) 
{
   options(NZDSN = dsn)
}

#-------------------------------------------------------------------------------

#' \code{db.set.nzcol.types} sets the option \code{NZCOL.TYPES} that is passed to 
#' \code{\link{db.nzquery}} as the default for the \code{col.types} parameter. 
#' This utility function generates both variants of the column names: the
#' original column names using underscores, as well as the naming convention 
#' using dots (when \code{convert.names} option in \code{db.nzquery} is set to TRUE).
#' @usage 
#' db.set.nzcol.types(col.types = list(
#'             user_hash      = "int64"
#'             , run_id       = "int64"
#'             , uid          = "int64"
#'             , mmdef_uid    = "int64"
#'             , config_uid   = "int64"
#'             , ao_run_id    = "int64"
#'             , to_date      = "date"
#'             , file_date    = "date"
#'             , dfp_file_date = "date"
#'             , an_file_date = "date"
#'             , ads_file_date = "date"
#'       ))
#' 
#' @rdname db.nzquery
db.set.nzcol.types <- function(col.types = list(
            user_hash      = "int64"
            , run_id       = "int64"
            , uid          = "int64"
            , mmdef_uid    = "int64"
            , config_uid   = "int64"
            , ao_run_id    = "int64"
            , to_date      = "date"
            , file_date    = "date"
            , dfp_file_date = "date"
            , an_file_date = "date"
            , ads_file_date = "date"
      ))
{
   # Generate all unique variants with '.' and '_'.
   col.names <- names(col.types)
   col.names.dots <- gsub("_", ".", tolower(col.names))
   col.names.dash <- gsub("\\.", "_", tolower(col.names.dots))
   lst.dots <- col.types
   lst.dash <- col.types
   names(lst.dots) <- col.names.dots
   names(lst.dash) <- col.names.dash
   lst <- c(lst.dash, lst.dots)
   lst.names <- names(lst)
   
   # Just keep unique names
   options(NZCOL.TYPES = lst[match(unique(lst.names), lst.names)])
}

#-------------------------------------------------------------------------------

#' \code{db.nzinfo} - Print connection information about the dsn. 
#' @rdname db.nzquery
db.nzinfo <- function(dsn = db.get.nzdsn(caller = "db.nzinfo"))
{
   df <- db.db.dsn.attr(dsn = dsn)
   writeLines(db.format.df(data.frame(FIELD = names(df), VALUE = t(df))))
   invisible(df)
}

#-------------------------------------------------------------------------------

#' \code{db.nzexist.tables} - checks if the tables with the specified names exist (views and synonyms will match too).  
#' Returns a vector of logical TRUE/FALSE values of the same length as \code{tab.names}. 
#' @param tab.names A vector of table names. The names must be local to the DSN, i.e., "analytics..impression" will always return FALSE.
#' @param dsn Connection DSN. By default will attempt to get the option NZDSN value, and will fail if the value is not found.
#' @name db.nzdml
#' @title Utilities for manipulating the database.
#' @rdname db.nzdml
db.nzexist.tables <- function(tab.names, dsn = db.get.nzdsn("db.nzexist.tables"))
{
   nz.tables <- db.nzshow.tabinfo(dsn = dsn, with.extras = FALSE)
   ix <- match(toupper(tab.names), toupper(nz.tables$NAME))
   return(!is.na(ix))   
}

#-------------------------------------------------------------------------------

#' \code{db.nzshow.tabinfo} - retrieves a data frame containing names and information for tables, 
#' views and synonyms.
#' @param with.extras When \code{TRUE}, the results include number of rows and storage size for tables 
#'        and referenced objects for synonyms.
#' @examples 
#' \dontrun{
#'    print(db.nzshow.tabinfo(dsn = "NZS", with.extras=T), right = F)
#' }
#' @rdname db.nzdml
db.nzshow.tabinfo <- function(dsn = db.get.nzdsn("db.nzshow.tabinfo"), with.extras = TRUE)
{
   if (with.extras)
   {
      sql.size <- 
      "select distinct 
           r.objname as name
         , r.objtype as type
         , r.owner as owner
         , r.createdate
         , t.reltuples as rowcount
         , round(t.used_bytes / 1024 / 1024, 0) as used_mbytes
         , t.skew
         , s.refdatabase ||'..'||s.refobjname as refobject  
      from 
            _v_obj_relation r 
         left join 
            _v_synonym s 
         on r.objid = s.objid
         left join 
            _v_table_only_storage_stat t      
         on 
            r.objid = t.objid 
      where r.objclass in (4905,4906,4907,4908,4909,4911,4913,4940,4953) 
      order by name
      "
      return(db.nzquery(sql.size, dsn = dsn))
   }
   
   db.nzquery("select distinct objname as name, objtype as type, owner as owner, createdate  
         from _v_obj_relation where objclass in (4905,4906,4907,4908,4909,4911,4913,4940,4953) order by name", dsn = dsn)
}

#-------------------------------------------------------------------------------

#' \code{db.nzshow.objects} - retrieves a vector of names of tables, views and synonyms 
#' matching a given pattern in the given DSN. Returns in disconnected state.
#' @param pattern A pattern to match to the table names (perl regex syntax).
#' @inheritParams db.nzexist.tables
#' @rdname db.nzdml
db.nzshow.objects <- function(pattern, dsn = db.get.nzdsn("db.nzshow.objects"))
{
   nz.tables <- db.nzshow.tabinfo(dsn = dsn, with.extras = FALSE)
   res <- grep(pattern, nz.tables$NAME, ignore.case = T, perl = T, value = T)
   return(res)
}

#-------------------------------------------------------------------------------

#' \code{db.nzshow.tables} - retrieves a vector of table names matching a given pattern
#' in the specified DSN. Returns in disconnected state.
#' @param pattern A pattern to match to the table names (perl regex syntax).
#' @inheritParams db.nzexist.tables
#' @rdname db.nzdml
db.nzshow.tables <- function(pattern = NULL, dsn = db.get.nzdsn("db.nzshow.tables"))
{
   nz.tables <- db.nzquery("select distinct tablename from _v_table where objclass in (4905,4911,4953,4961) order by tablename", 
         dsn = dsn)$TABLENAME
   if (!is.null(pattern))
   {
      res <- grep(pattern, nz.tables, ignore.case = T, perl = T, value = T)
      return(res)      
   }
   return(nz.tables)
}

#-------------------------------------------------------------------------------

#' \code{db.nzdrop.tables} - drops tables listed in tab.names, checking first if the tables exist. 
#' @param tab.names A vector of table names.
#' @param dsn In \code{db.nzdrop.tables} and \code{db.delete.records}, 
#'        connection DSN, which must be specified by the caller (no default for a bit more safety).
#' @rdname db.nzdml
db.nzdrop.tables <- function(tab.names, 
      dsn = stop("db.nzdrop.tables: 'dsn' must be specified explicitly."),
      verbose = FALSE)
{
   # Drop any tables that already exist
   ix.exist <- db.nzexist.tables(tab.names = tab.names, dsn = dsn)
   if (sum(ix.exist) > 0)
   {      
      db.log(1, paste("DROPPING TABLES: ", db.quote.csv(tab.names[ix.exist])))
      sql <- paste("drop table", tab.names[ix.exist])
      res <- db.nzexec(sql, dsn = dsn, verbose = verbose)
   }
}

#-------------------------------------------------------------------------------

#' \code{db.nzdelete.bykey} - delete all records from the specified table that 
#' match the specified list of keys. 
#' @param tab.name Table name.
#' @param pklist A list, containing the keys. Each item's name must match 
#'         a column in the table, and its value is the value to filter by.
#'         The items in the list can use the types provided by 
#'         \code{\link{db.cast}} for correct quoting within SQL.
#' @param verbose If \code{TRUE} prints the query and timing.
#' @inheritParams db.nzdrop.tables
#' @examples 
#' \dontrun{
#' # > db.delete.bykey("mytab", list(name = "somename", 
#' # + to_date = db.cast("2011-12-11", "date")), dsn="NZS", verbose=T)
#' # [1] "delete from mytab where name = 'somename' and to_date = '2011-12-11'::date"
#' }
#' @seealso \code{\link{db.quote.sql}}, \code{\link{db.cast}}
#' @rdname db.nzdml
db.nzdelete.bykey <- function(tab.name, pklist, verbose = FALSE,
      dsn = stop("db.delete.records: 'dsn' must be specified explicitly."))
{
   cols <- names(pklist)
   vals <- lapply(pklist, function(x) db.quote.sql(x))
   filters <- paste(paste(cols, "=", vals), collapse = " and ")
   sql <- paste("delete from ", tab.name, " where ", filters, sep="")
   db.nzquery(sql, dsn = dsn, verbose = verbose)
}

#-------------------------------------------------------------------------------

#' \code{db.nzexists.bykey} - checks if there are records in the specified table
#' that match the specified list of keys 
#' @inheritParams db.delete.bykey
#' @param stop.if.any \code{db.nzexists.bykey} stops if any rows matched the key values in \code{pklist}. 
#'    \code{db.nzdata.check} stops if \code{value} exists in the given table and column.
#' @param stop.if.none \code{db.nzexists.bykey} stops if no rows matched the key values in \code{pklist}.
#'    \code{db.nzdata.check} stops if \code{value} does not exist in the given table and column.
#' @rdname db.nzdml
db.nzexists.bykey <- function(tab.name
   , pklist
   , verbose = FALSE
   , dsn = db.get.nzdsn("db.nzexists.bykey")
   , stop.if.any = FALSE   # if TRUE, then stop if this value exists
   , stop.if.none = FALSE  # if TRUE, then stop if this value does not exist
)
{
   cols <- names(pklist)
   vals <- lapply(pklist, function(x) db.quote.sql(x))
   filters <- paste(paste(cols, "=", vals), collapse = " and ")
   sql <- paste("select count(*) as cnt from  (select ", cols[1], " from ", tab.name, " where ", filters, " limit 1) a", sep="")
   res <- db.nzquery(sql, dsn = dsn, verbose = verbose)
   
   exists <- (res$CNT[1] > 0)

   # Stop if the value does not exist
   if (stop.if.none && !exists)
   {
      stop(paste("db.nzexists.bykey: No values in table '", tab.name, "' matching the filters: (", filters, "); stopping", sep = ""))      
   }
   
   # Stop if the value exists   
   if (stop.if.any && exists)
   {
      stop(paste("db.nzexists.bykey: Found values in table '", tab.name, "' matching the filters: (", filters, "); stopping", sep = ""))      
   }
   
   return(exists)
}


#-------------------------------------------------------------------------------

#' \code{db.nzdata.check} - Determines if data exists in a given table for a given value of a given column. 
#' See \code{db.nzexists.bykey} for more general functionality.
#' @param table.name Name of the table to query.
#' @param column.name Name of the column to check for the \code{value}.
#' @param value Value that is checked for.
# @param stop.if.any If \code{TRUE}, then stop if this value exists.
# @param stop.if.none If \code{TRUE}, then stop if this value does not exist.
#' @rdname db.nzdml
db.nzdata.check <- function(
      table.name              # Name of table to query 
      , column.name           # Column name to look for value
      , value                 # Value to look for (must be typed appropriately)
      , stop.if.any = FALSE   # if TRUE, then stop if this value exists
      , stop.if.none = FALSE  # if TRUE, then stop if this value does not exist
      , dsn = db.get.nzdsn(caller = "db.nzdata.check")
) {

   pklist <- list(x = value)
   names(pklist) = column.name
   db.nzexists.bykey(tab.name = table.name, pklist = pklist, 
         dsn = dsn,
         stop.if.any = stop.if.any,
         stop.if.none = stop.if.none)
}

#-------------------------------------------------------------------------------

#' \code{db.nzinsert.sql} - generate SQL to insert records from \code{pklist} into the specified table.
#' In this function, \code{pklist} can be a list of vectors or a data frame as long 
#' as the column names of the data frame match the column names in the table; 
#' then all rows are inserted in a single batch using \code{insert into ... (...) values (...)}. 
#' All \code{'.'} characters in the list names are converted to \code{'_'}. When creating
#' a \code{data.frame} to pass to this function, be careful to use \code{stringsAsFactors = FALSE}
#' to ensure proper quoting of the values.
#' Returns a vector of SQL statements.
#' @inheritParams db.delete.bykey
#' @seealso \code{\link{db.write.nz}}
#' @rdname db.nzdml
db.nzinsert.sql <- function(tab.name, pklist)
{
   if (!is.list(pklist)) stop("db.nzinsert.sql: pklist is not a list")
   llen <- unlist(lapply(pklist, function(x) length(x)))
   if (any(llen == 0) || any(diff(llen) != 0)) 
   {
      stop(paste("db.nzinsert.sql: pklist invalid - found records that are empty or of different lengths."))
   }
   len <- llen[1]
   
   # Comma-separated column names
   cols <- paste(gsub("\\.", "_", names(pklist)), collapse=", ")
   # apply appropriate quotes to values; still a list
   vals <- lapply(pklist, function(x) db.quote.sql(x))
   # convert to a vector of comma-separated lists of values
   sql.vals <- unlist(lapply(1:len, function(i) paste(unlist(lapply(vals, function(x) x[i])), collapse = ", ")))
   # generate a vector of sql statements
   sql <- paste("insert into ", tab.name, " (", cols, ") values (", sql.vals, ")", sep="")
   return(sql)
}

#-------------------------------------------------------------------------------

#' \code{db.nzinsert} - insert records from \code{pklist} into the specified table.
#' In this function, \code{pklist} can be a list of vectors or a data frame as long 
#' as the column names of the data frame match the column names in the table; 
#' then all rows are inserted in a single batch using 
#' 
#' \code{insert into ... (...) values (...)}. 
#' Returns the (invisible) number of rows inserted.
#' @param gen.stats Flag indicating whether statistics should be run on the table after inserting.
#' @inheritParams db.delete.bykey
#' @rdname db.nzdml
db.nzinsert <- function(tab.name, pklist, verbose = FALSE, gen.stats = FALSE,
      dsn = stop("db.nzinsert: 'dsn' must be specified explicitly."))
{
   sql <- db.nzinsert.sql(tab.name, pklist)
   len <- length(sql)
   # run the inserts
   if (verbose) tic(paste("Inserting ", len, " records into ", tab.name, sep=""))
   db.nzexec(sql, dsn = dsn, verbose = verbose)
   if (verbose) toc()
   if (gen.stats)
   {
      if (verbose) tic(paste("Generating stats for ", tab.name, sep=""))
      db.nzquery(paste("generate statistics on", tab.name), dsn = dsn, verbose = verbose)
      if (verbose) toc()      
   }   
   invisible(len)
}

#-------------------------------------------------------------------------------

#' \code{db.create.robjects} - creates a table for storing 
#' binary data (mostly for persisting R objects) and a view that excludes the actual data for convienience.
#' 
#' The \code{tab.robjects} table has the following fields:
#' \itemize{
#' \item{uid: bigint - }{Unique identifier for the record. Uniqueness models may vary from one application to another, 
#'                       for example, the combination of \code{name + version} may also be made unique by construction.}
#' \item{name varchar(64) - }{User-provided name of the record}
#' \item{version varchar(64) - }{User-provided version string}
#' \item{ts timestamp - }{timestamp, inserted automatically}
#' \item{owner_name varchar(64) - }{Netezza or Unix or free form?}
#' \item{label varchar(256) - }{free-form label provided by the user}
#' \item{description varchar(256) - }{more free-form info}
#' \item{object_info varchar(256) - }{automatically extracted information about the objects contained in the record}
# \item{session_user varchar(64) - }{Netezza session user name}
# \item{current_user varchar(64) - }{Netezza current user name}
#' \item{seqno integer - }{sequential number of data chunk} 
#' \item{chunk varchar(64000) - }{the "LOB", storing the a chunk of data for the object, can be multiple per id}
#' }
#' @param tab.robjects Name of the table that stores the objects. The view created has the same name with
#'        a suffix \code{_v}.  
#' @name db.nzsave.rdata
#' @title Save and restore R objects to/from the database.        
#' @rdname db.nzsave.rdata
db.create.robjects <- function(tab.robjects, dsn = db.get.nzdsn("db.create.robjects"))
{
   db.nzquery(
         paste("create table ", tab.robjects, "(",               
               "  uid bigint",               # TODO: have another facility for getting an id from a global sequence
               ", name varchar(64)",         # User-provided name of the record
               ", version varchar(64)",      # User-provided version string      
               ", ts timestamp default now()",             # timestamp
               ", owner_name varchar(64)",   # Netezza or Unix or free form?
               ", label varchar(256)",       # free-form label provided by the user
               ", description varchar(256)", # more free-form info
               ", object_info varchar(256)", # automatically extracted information about the objects contained in the record
               #", session_uname varchar(64)",# Netezza session user name
               #", current_uname varchar(64)",# Netezza current user name
               ", seqno integer",            # sequential number of data chunk 
               ", chunk varchar(64000)",     # the "LOB", storing the a chunk of data for the object, can be multiple per id  
               ") distribute on random",
               sep = ""),
         dsn = dsn
   )
   
   db.nzquery(paste("
create or replace view ", tab.robjects, "_v as 
select uid
, name
, version
, ts
, owner_name
, label
, description
, object_info
, count(*) as nchunks
, sum(length(chunk)) as totsize 
from ", tab.robjects, "  
group by uid, name, version, ts, owner_name, label, description, object_info"
   , sep=""), dsn = dsn)
}

#-------------------------------------------------------------------------------

#' \code{db.nzsave.rdata} - saves the objects listed in the arguments to the tab.robjects table in base64
#' encoding. Uses save() -> uuencode -> read.csv() -> insert to Netezza. 
#' @param uid Unique identifier for the table (long integer). Uniqueness model may vary among applications, 
#'        for example, \code{name + version} could be made unique by construction. 
#'        It is the responsibility of the caller to make sure that the keys are indeed unique.
#' @param tab.robjects Name of the table that stores the objects.
#' @param overwrite When \code{TRUE}, deletes the record with the given \code{uid} if it exists.
#'        When \code{FALSE} and a record with the given \code{uid} already exists, throws an error.
#' @param owner.name Record annotation. Name of the record owner, not linked to any permissions.
#' @param name Annotation. User-provided name of the object.
#' @param version Annotation. User-provided version string.
#' @param label Annotation. Free-form label for the record. This can be up to 255 characters, but the intent 
#'        for this field is to keep it short. 
#' @param description Annotation. A description of what's in the record up to 255 characters. 
#' @param dsn Connection DSN. If not supplied, will try to find the default in \code{options("NZDSN")}.
#' @param tmpdir Temporary directory used to save the R objects and convert them to uuencoded text.
#' @param envir (\code{db.nzsave.rdata}) Environment to search for objects to be saved.
#' @return \code{db.nzsave.rdata} - (Invisible) vector of names of the saved objects.
#' @seealso \code{\link{db.uid}}
#' @rdname db.nzsave.rdata
db.nzsave.rdata <- function(..., uid, tab.robjects = "robjects", overwrite = FALSE,
      owner.name = System$getUsername(), name = "", version = "", label = "", description = "", 
      dsn = db.get.nzdsn("db.nzsave.rdata"),
      tmpdir = "/tmp", envir = sys.frame(sys.parent()))
{
   file.r <- paste(tmpdir, "/robj_", uid, "_", floor(runif(1) * 1000000), ".Rdata", sep="")
   
   obj.names <- as.character(substitute(list(...)))[-1L]
   obj.types <- unlist(lapply(obj.names, function(s) typeof(get(s, envir=envir))))
   obj.class <- unlist(lapply(obj.names, function(s) class(get(s, envir=envir))))
   obj.info  <- paste(paste(obj.names, "::", obj.class, "|", obj.types, sep=""), collapse=", ")
   
   save(..., file = file.r, envir = envir)
   file.uu <- paste(file.r, ".uu", sep="")
   
   # Encode in ascii
   sys <- Sys.info()[["sysname"]]
   command <- switch(sys, 
         Darwin = paste("uuencode -m -o ", file.uu, " " , file.r, " ", file.r, sep=""),
         Linux = paste("uuencode -m ", file.r, " ", file.r, " > " , file.uu, sep=""))
   #command <- paste("uuencode -m -o ", file.uu, " " , file.r, " ", file.r, sep="") 
   res <- run.remote(command, remote="")
   if (res$cmd.error)
   {
      stop(paste("db.nzsave.rdata: uuencode failed:", res$warn.msg))
   }
   
   # Read it in.
   frm <- read.csv(file.uu, header=F, stringsAsFactors=F)  # small, so no need to use db.read.csv
   
   # Remove files
   unlink(file.r)
   unlink(file.uu)
   
   # Convert to bigger chunks
   chunk.size = 50000
   s <- paste(frm[[1]], collapse="\n")
   nc <- nchar(s)
   nchunks <- floor(nc / chunk.size) + 1
   chunks <- unlist(lapply(1:nchunks, function(i) substr(s, (i - 1) * chunk.size + 1, i * chunk.size)))
   
   # Stick it in Netezza   
   n <- length(chunks)
   recs <- db.nzquery(paste("select count(*) as cnt from ", tab.robjects, " where uid = ", uid, sep=""), dsn = dsn)
   if (recs$CNT[1] > 0)
   {
      if (overwrite)
      {
         res <- db.nzquery(paste("delete from ", tab.robjects, " where uid = ", uid, sep=""), dsn = dsn)            
      }
      else 
      {
         stop(paste("db.nzsave.rdata: the uid = ", uid, " already exists in table ", tab.robjects))
      }
   }   
   sql <- paste("insert into ", tab.robjects, 
         " (uid, name, version, ts, owner_name, label, description, object_info, seqno, chunk) values (", 
         rep(uid, n),
         ", '", name, "'",
         ", '", version, "'",
         ", now()", 
         ", '", owner.name, "'",
         ", '", label, "'",
         ", '", description, "'",
         ", '", obj.info, "'",
         #session_user(),
         #current_user(),
         ", ", 1:n, 
         ", '", chunks, "')", sep="")
   res <- db.nzexec(sql, dsn=dsn)   
   
   if (0) frm <- data.frame(
           uid = rep(uid, n)
         , name = rep(name, n)
         , version = rep(version, n)
         , ts = rep(as.character(Sys.time()), n)
         , owner_name = rep(owner_name, n)
         , label = rep(label, n)
         , description = rep(description, n)
         , object_info = rep(obj.info, n)
         , seqno = 1:n
         , chunk = chunks                  
         )
   invisible(obj.names)
}

#-------------------------------------------------------------------------------

#' \code{db.nzload.rdata} - retrieves and decodes R objects from the database.
#' @inheritParams db.nzsave.rdata 
#' @param envir (\code{db.nzload.rdata}) Environment where the data should be loaded.
#' @return \code{db.nzload.rdata} - (Invisible) vector of names of the loaded objects.
#' @rdname db.nzsave.rdata
db.nzload.rdata <- function(uid, tab.robjects = "robjects", 
      dsn = db.get.nzdsn("db.nzload.rdata"),
      tmpdir = "/tmp", envir = sys.frame(sys.parent()))
{
   sql <- paste("select chunk from ", tab.robjects, " where uid = ", uid, " order by seqno")
   frm <- db.nzquery(sql, dsn=dsn)
   
   if (is.null(frm) || is.na(frm) || nrow(frm) == 0) 
   {
      stop(paste("db.nzload.rdata: failed to fetch data from the database.\n", sql, sep=""))
   }
   
   file.r <- paste(tmpdir, "/robj_", uid, "_", floor(runif(1) * 1000000), ".Rdata", sep="")
   file.uu <- paste(file.r, ".uu", sep="")
   
   cat(paste(frm[[1]], collapse=""), "\n", file = file.uu, sep="")
   command <- paste("uudecode -o ", file.r, " ", file.uu, sep="")
   res <- run.remote(command, remote="")
   if (res$cmd.error)
   {
      stop(paste("db.nzload.rdata: uudecode failed:", res$warn.msg))
   }
   
   obj.names <- load(file.r, envir = envir)
   unlink(file.r)
   unlink(file.uu)
   invisible(obj.names)
}

#-------------------------------------------------------------------------------

#' \code{db.nzload.rdata.ex} - Loads an R object from the \code{tab.robjects} table that matches
#' the provided \code{where.*} filters and is the first in sorting order specified by 
#' \code{col.max}, i.e., gets the record for the largest (assuming that it's also most recent) UID.
#' If "most recent" needs to be by the timestamp, set \code{col.max = "ts"}. It is also possible to 
#' use multiple columns, e.g., \code{col.max = c("version", "uid")} will return a row with the max 
#' version and then max UID, but beware of string-based sorting of versions and other columns.
#' This function can also be used to retrieve metadata for all matching records, 
#' by setting \code{do.load = FALSE} and \code{col.max = NULL}.
#' @param where.* Filters that have to match exactly when non-null.
#' @param col.max The column(s) specifying the descending sort order, of which only the max (first) row is used.
#' @param do.load Flag indicating whether the R object should be loaded. When \code{TRUE}, \code{col.max} must
#'        be one or more of the allowed columns: \code{c("uid", "ts", "name",} \code{"version", "label")}.
#' @return \code{db.nzload.rdata.ex} - A one-row (default) or multi-row data frame containing the meta 
#' data for the selected row(s). 
#' @rdname db.nzsave.rdata
db.nzload.rdata.ex <- function(tab.robjects = "robjects",       
      dsn = db.get.nzdsn("db.nzload.rdata"),
      where.name = NULL, where.version = NULL, where.owner = NULL, where.label = NULL, col.max = "uid",
      do.load = TRUE, tmpdir = "/tmp", envir = sys.frame(sys.parent()))
{   
   filter <- unlist(list(
                 name       = db.cast(where.name      , "character")
               , version    = db.cast(where.version   , "character")
               , owner_name = db.cast(where.owner     , "character")
               , label      = db.cast(where.label     , "character")
               ))
   if (length(filter) > 0)
   {
      cols <- names(filter)
      vals <- unlist(lapply(filter, function(x) db.quote.sql(x)))
      where.sql <- paste(paste(cols, "=", vals), collapse = " and ") 
      where.sql <- paste("where", where.sql)
   }
   else where.sql = ""
   
   sql <- paste("select uid::varchar(16) as uid, name, version, ts, owner_name, label, description, object_info, nchunks, totsize from ", tab.robjects, 
         "_v ", where.sql, sep="")
   # This is somewhat weird - we allow multiple col.max values, all of which will be sorted in descending order
   # and only the top row is returned. 
   if (!is.null(col.max) && !any(is.na(match(tolower(col.max), c("uid", "ts", "name", "version", "label")))))
   {
      sql <- paste(sql, " order by ", paste(col.max, collapse=" desc, "), " desc limit 1", sep="")
   }
   else if (do.load) stop(paste("db.nzload.rdata.ex: col.max is invalid, can't load data."))

   # Get the uid for the config.
   
   frm <- db.nzquery(sql, dsn = dsn)
   if (length(frm$UID) == 0) 
   {
      stop(paste("db.nzload.rdata.ex: Can't find data in table ", tab.robjects, "_v ", where.sql, sep=""))
   }
   if (do.load) db.nzload.rdata(as.int64(frm$UID[1]), tab.robjects = tab.robjects, dsn = dsn, tmpdir = tmpdir, envir = envir)
   
   return(frm)
}

#-------------------------------------------------------------------------------

#' Runs various checks on the integrity of a table. If any of the arguments are NULL then that check is not done.
#' @param table.name            Name of the table.
#' @param filter.lst            Table will be filtered on this list.
#' @param min.rows = 0          Minimum number of rows the table should have.
#' @param max.rows = Inf        Maximum number of rows the table should have.
#' @param pk.set = NULL         Columns that define the primary key (1 row per table), note these will also be checked for NAs.
#' @param not.na = NULL         Columns that should never be NA.
#' @param not.negative = NULL   Columns that should never be negative, note these will also be checked for NAs.
#' @param positive = NULL       Columns that should be strictly positive, note these will also be checked for NAs.
#' @param required.cols         Always required columns (will be checked for NAs).
#' @param print.rows = 20       Number of rows to print when primary key constraint is invalid.
#' @param error.func = stop     Function f(string) to call in case of error. An alternative is error.func.
#' @param dsn                   Connection DSN.
#' @note The columns can be entered as column.name, COLUMN.NAME, column_name or COLUMN_NAME 
#' @name db.nztable.integrity
#' @aliases db.nztable.integrity
#' @title Check integrity of a table
#' @rdname db.nztable.integrity
db.nztable.integrity <- function(
      table.name              # Name of the table
      , filter.lst            # Table will be filtered on this list.
      , min.rows = 0          # Minimum number of rows the table should have
      , max.rows = Inf        # Maximum number of rows the table should have
      # If any of these are NULL then that check is not done
      # Note that these can be entered as column.name, COLUMN.NAME, column_name or COLUMN_NAME 
      , pk.set = NULL         # Columns that define the primary key (1 row per table), note these will also be checked for NAs
      , not.na = NULL         # Columns that should never be NA
      , not.negative = NULL   # Columns that should never be negative, note these will also be checked for NAs
      , positive = NULL       # Columns that should be strictly positive, note these will also be checked for NAs
      , required.cols = c("run_id", "network_id", "to_date")   # Always required columns (will be checked for NAs)  
      , print.rows = 20       # Number of rows to print when primary key constraint is invalid
      , error.func = stop     # function f(string) to call in case of error. An alternative is error.func.
      , dsn = db.get.nzdsn(caller = "db.nztable.integrity")
) 
{
   
   filter.cols <- toupper(gsub("\\.", "_", names(filter.lst)))   # convert all dots to "_"
   filter.vals <- lapply(filter.lst, function(x) db.quote.sql(x))
   filters <- paste(paste(filter.cols, "=", filter.vals), collapse = " and ")
   filters <- paste("(", filters, ")", sep = "")   # enclose in parens
   #sql <- paste("select count(*) as cnt from  (select ", cols[1], " from ", tab.name, " where ", filters, " limit 1) a", sep="")   
   
   # Also check pk.set for NAs
   not.na <- unique(c(not.na, pk.set, not.negative, positive, required.cols))
   
   # Determine which checks to run
   run.size <- min.rows > 0 | max.rows < Inf  
   run.pk <- !is.null(pk.set)
   run.na <- !is.null(not.na)
   run.neg <- !is.null(not.negative)
   run.pos <- !is.null(positive)
   
   # Convert into COLUMN_NAME form
   if (run.pk) pk.set <- toupper(gsub("\\.", "_", pk.set))
   if (run.na) not.na <- toupper(gsub("\\.", "_", not.na))
   if (run.neg) not.negative <- toupper(gsub("\\.", "_", not.negative))
   if (run.pos) positive <- toupper(gsub("\\.", "_", positive))
   
   # Combined columns
   all.cols <- unique(c(pk.set, not.na, not.negative, positive))
   
   # Check table existence
   if (!db.nzexist.tables(table.name, dsn = dsn))
      error.func(paste("db.nztable.integrity: ", table.name, " does not exist", sep = ""))
   
   # Check column existence
   table.cols <- names(db.nzquery(paste("select * from ", table.name, " limit 0", sep = ""), dsn = dsn))
   exists <- all.cols %in% table.cols
   if (!all(exists))
      error.func(paste("db.nztable.integrity: ", table.name, " is missing columns: ", paste(all.cols[!exists], collapse = ", "), sep = ""))
   
   # Check the primary key of the table
   if (run.pk) {
      
      # Grab the top violating rows
      frm.pk <- db.nzquery(paste("
         with
         base as
         (select
            ", paste(pk.set, collapse = ", "), ",
            count(*) as num_rows
         from
            ", table.name, "
         where
            ", filters, "
         group by
            ", paste(pk.set, collapse = ", "), ")
         select * from base where num_rows > 1
         order by num_rows desc
         limit ", print.rows, sep = ""), dsn = dsn) 
      
      # Check for failure and print top results 
      if (nrow(frm.pk) > 0) {
         
         # Collapse the data.frame for printing
         collapsed.df <- db.collapse.df(frm.pk) 
         
         # Construct error message
         error.message <- paste("db.nztable.integrity: ", table.name, " violated primary key constraint:\n", collapsed.df, sep = "")
         
         # Stop with error message
         error.func(error.message)
      }
   }
   
   # Need to run at least one aggregate
   if (run.size | run.na | run.neg | run.pos) {
      
      # Start body
      SQL.body <- "count(*) as NROWS"
      
      # Append on queries to solve for NA, neg & pos
      if(run.na) SQL.body <- paste(SQL.body, paste(paste("sum(nvl2(", not.na, ", 0, 1)) as ", not.na, "_NA", sep = ""), collapse = ", "), sep = ", ")
      if(run.neg) SQL.body <- paste(SQL.body, paste(paste("min(", not.negative, ") as ", not.negative, "_NEG", sep = ""), collapse = ", "), sep = ", ")
      if(run.pos) SQL.body <- paste(SQL.body, paste(paste("min(", positive, ") as ", positive, "_POS", sep = ""), collapse = ", "), sep = ", ")
      
      # Combine into single SQL statement
      SQL <- paste("
         select
            ", SQL.body, "
         from
            ", table.name, "
         where
            ", filters, "
         ", sep = "") 
      
      # Execute SQL
      frm.agg <- db.nzquery(SQL, dsn = dsn)
      
      # Run all tests
      if (run.size) {
         if (frm.agg$NROWS < min.rows)
            error.func(paste("db.nztable.integrity: ", table.name, " has only ", frm.agg$NROWS, " rows, less than required of ", min.rows, sep = ""))
         if (frm.agg$NROWS > max.rows)
            error.func(paste("db.nztable.integrity: ", table.name, " has ", frm.agg$NROWS, " rows, more than allowed of ", max.rows, sep = ""))
      }
      if (frm.agg$NROWS > 0) {
         if (run.na) {
            na.counts <- frm.agg[, paste(not.na, "_NA", sep = ""), drop = FALSE]
            for (i in length(not.na))
               if (na.counts[i] > 0)
                  error.func(paste("db.nztable.integrity: ", table.name, " has column ", not.na[i], " with ", na.counts[i], " NAs", sep = ""))
         }
         if (run.neg) {
            neg.mins <- frm.agg[, paste(not.negative, "_NEG", sep = ""), drop = FALSE]
            for (i in length(not.negative))
               if (neg.mins[i] < 0)
                  error.func(paste("db.nztable.integrity: ", table.name, " has column ", not.negative[i], " with values as small as ", neg.mins[i], sep = ""))
         }
         if (run.pos) {
            pos.mins <- frm.agg[, paste(positive, "_POS", sep = ""), drop = FALSE]
            for (i in length(positive))
               if (pos.mins[i] <= 0)
                  error.func(paste("db.nztable.integrity: ", table.name, " has column ", positive[i], " with values as small as ", pos.mins[i], sep = ""))
         }      
      }
   }
}

#-------------------------------------------------------------------------------

#' Test create, insert, delete, update, alter, drop, groom table; create/drop view and synonym.
#' Doesn't test cross-user operations.
#' @name db.nztest
#' @aliases db.nztest.user.permissions
#' @title Various tests.
#' @rdname db.nztest
db.nztest.user.permissions <- function(dsn)
{
   db.nzexec(
         c("create table tmp_test_khabfg (a int, b int)", 
               "insert into tmp_test_khabfg (a, b) values (1, 2)", 
               "insert into tmp_test_khabfg (a, b) values (3, 4)", 
               "insert into tmp_test_khabfg (a, b) values (5, 6)", 
               "delete from tmp_test_khabfg where a = 5", 
               "update tmp_test_khabfg set b = 8 where a = 3", 
               "select * from tmp_test_khabfg", 
               "alter table tmp_test_khabfg rename to tmp_test_khabfg_orig", 
               "create synonym tmp_test_khabfg for tmp_test_khabfg_orig", 
               "create or replace view tmp_test_khabfg_v as select a, b from tmp_test_khabfg where b > 2", 
               "select * from tmp_test_khabfg_v", 
               "select * from tmp_test_khabfg", 
               "truncate table tmp_test_khabfg_orig", 
               "drop view tmp_test_khabfg_v", 
               "groom table tmp_test_khabfg_orig", 
               "generate statistics on tmp_test_khabfg_orig", 
               "drop synonym tmp_test_khabfg", 
               "drop table tmp_test_khabfg_orig"), dsn = dsn, verbose = T)
}

#-------------------------------------------------------------------------------

#' \code{db.nzfind.date.all} returns a vector of all dates for which there's data in the given table
#' within the lookback window.
#' @param tab.name Name of the table to check.
#' @param dsn Database connection DSN
#' @param date.col Name of the column containing the date field.
#' @param max.lookback Maximum lookback window in days.
#' @param verbose If TRUE, prints and times SQL queries.
#' @name db.nzfind.date
#' @aliases db.nzfind
#' @title Quickly find specific dates in a table.
#' @rdname db.nzfind.date
db.nzfind.date.all <- function(tab.name, dsn = "NZDS_ERNIE", date.col = 'to_date', 
      max.lookback = 180, verbose = FALSE)
{
   sql.vec <- paste("select ", date.col, " as to_date from ", tab.name, " where ", date.col, 
         " = CURRENT_DATE - ", 1:max.lookback, " limit 1", sep = "")
   res <- db.nzexec(sql.vec, dsn = dsn, verbose = verbose)
   
   # Can't collapse a list of dates into a vector of dates, so have to jump through some hoops.
   res.len <- length(res)
   dat.len <- length(unlist(res))
   res.dates <- vector(dat.len, mode="integer")
   class(res.dates) = class(Sys.Date())
   j <- 1 
   for (i in 1:res.len) 
   { 
      if (length(res[[i]]$TO_DATE)) 
      { 
         res.dates[j] <- res[[i]]$TO_DATE[1]; 
         j <- j + 1; 
      } 
   }
   
   return(res.dates)
}

#-------------------------------------------------------------------------------

#' \code{db.nzcheck.date} checks existence of data for the CURRENT_DATE - offset date. 
#' Returns the date or NULL if not found.
#' @rdname db.nzfind.date
db.nzfind.date <- function(offset, tab.name, dsn = "NZDS_ERNIE", date.col = 'to_date')
{
   sql <- paste("select ", date.col, " as to_date from ", tab.name, " where ", date.col, 
         " = CURRENT_DATE - ", offset, " limit 1", sep = "")
   res <- db.nzquery(sql, dsn = dsn, verbose = T)
   if (length(res$TO_DATE)) 
   { 
      return(res$TO_DATE[1]); 
   }
   return(NULL)
}

#-------------------------------------------------------------------------------

#' \code{db.nzfind.date.last} finds the most recent date in the table up to the max.lookback window. 
#' Returns the date or NULL if not found.
#' @rdname db.nzfind.date
db.nzfind.date.last <- function(tab.name, dsn = "NZDS_ERNIE", date.col = 'to_date', max.lookback = 180)
{   
   for (i in 0:max.lookback) 
   {
      if (!is.null(this.date <- db.nzfind.date(i, tab.name = tab.name, dsn = dsn, date.col = date.col)))
      {
         return(this.date)
      }
   }   
   #warning(paste("db.nzfind.date.last: didn't find any dates in the ", max.lookback, " look-back window", sep=""))
   return(NULL)
}

#-------------------------------------------------------------------------------

#' db.nzget.ddl - Given a table name, generates the DDL to create the table. Does not include 'organize on'.
db.nzget.ddl <- function(table.name, dsn = db.get.nzdsn(), rename.to = table.name)
{
   if (!db.nzexist.tables(table.name, dsn = dsn))
   {
      stop(paste("db.nzget.ddl: table '", table.name, "' doesn't exist in DSN '", dsn, "'.", sep = ""))
   }
   
   df.ddl <- db.nzquery("
   select 
      '   ,' as comma, 
      c.attname, 
      upper(c.atttype) as atttype,
      attnotnull 
   from 
      _v_relation_column_def c, 
      _v_obj_relation r 
   where 
      c.objid = r.objid 
      and c.attnum > 0 
      and lower(r.objname) = lower(TABLE.NAME) 
   order by attnum
   ", var.list = list(TABLE.NAME = as.character(table.name))
    , dsn = dsn, verbose = FALSE, as.is = TRUE)  # as.is = TRUE so that R doesn't try to interpret empty strings as NA
   
   df.ddl[1, 1] <- ""
   ddl.body <- db.format.df(df.ddl, header.line = FALSE, header = FALSE)
   ddl.create <- paste("CREATE TABLE ", toupper(rename.to), " (", sep = "")
   ddl.footer <- ")"
   
   df.dist <- db.nzquery("
   select attname 
   from _v_table_dist_map 
   where 
      lower(tablename) = lower(TABLE.NAME) 
   order by distseqno
   ", var.list = list(TABLE.NAME = as.character(table.name)), dsn = dsn, verbose = FALSE)
   
   dist.keys <- df.dist[[1]]
   if (length(dist.keys) == 0)
   {
      ddl.distribute = "DISTRIBUTE ON RANDOM"   
   } else
   {
      ddl.distribute = paste("DISTRIBUTE ON (", toupper(paste(dist.keys, collapse = ", ")), ")", sep = "")
   }
   ddl <- paste(ddl.create, ddl.body, ddl.footer, ddl.distribute, sep = "\n")
   return(ddl)
}

#-------------------------------------------------------------------------------

#' db.nzget.ctas - Generates a SQL statement for a CTAS from SQL statement and distribution keys.
#' If distribute.on is NULL, default distribution is assumed (first column of the table).
db.nzget.ctas <- function(table.name, sql, distribute.on = "random")
{
   sql.create <- paste("CREATE TABLE ", toupper(table.name), " AS ", sep = "")
   
   if (is.null(distribute.on))
   {
      ddl.distribute = ""
   } else if (toupper(distribute.on) == "RANDOM")
   {
      ddl.distribute = "DISTRIBUTE ON RANDOM"   
   } else
   {
      ddl.distribute = paste("DISTRIBUTE ON (", toupper(distribute.on), ")", sep = "")
   }
   ddl <- paste(sql.create, sql, ddl.distribute, sep = "\n")
   return(ddl)
}


#-------------------------------------------------------------------------------

# DDL Doesn't work when src tab in another db.
#db.nzmigrate(tab.src = "datasci..dsproc_status", dsn.src = "NZS_ERNIE", tab.dest = "dsproc_status", dsn.dest = "NZS", verbose = T, remote.src = "sizrailev@ny7anx01", remote.dest = "sizrailev@devanx01")

# The following produces another kind of error:
#db.nzmigrate(tab.src = "dsproc_status", dsn.src = "NZDS_ERNIE", tab.dest = "dsproc_status", dsn.dest = "NZS", verbose = T, remote.src = "sizrailev@ny7anx01", remote.dest = "sizrailev@devanx01")
# HY000 45 ERROR:  Error: Reload allow NULLs mismatch.  Column 1.
# need to get a full DDL including NOT NULL

#' Migrate an existing table from one Netezza to another.
#' @param \code{tab.src} Name of source table. This must be local to the \code{dsn.src}, which is a known and annoying limitation.
#' @param \code{dsn.src} Source DSN.
#' @param \code{tab.dest} Name of destination table. This must be local to the \code{dsn.dest}.
#' @param \code{dsn.dest} Destination DSN.
#' @param \code{remote.src} Source remote machine, e.g., 'user@@ny7anx01'.
#' @param \code{remote.dest} Destination remote machine, e.g., 'user@@ny7anx01'. This can be the same as source remote.
#' @param \code{tmpdir.src} Directory for temporary files on source machine.
#' @param \code{tmpdir.dest} Directory for temporary files on destination machine.
db.nzmigrate <- function(tab.src, dsn.src, tab.dest, dsn.dest, 
      verbose = FALSE, remote.src = "", remote.dest = "", 
      tmpdir.src = "/data1/r_tmp_files", tmpdir.dest = tmpdir.src)
{        
   # Never replace the destination table. A more flexible solution would be 
   # an action create or insert, but just too dangerous, so always create.
   if (db.nzexist.tables(tab.dest, dsn = dsn.dest))
   {
      stop(paste("Table '", tab.dest, "' already exists in DSN '", dsn.dest, "'", sep = ""))
   }
   
   # Define SQL here - maybe later the function is going to be a lot more flexible and take an arbitrary SQL.
   query.sql <- paste("select * from ", tab.src, sep = "")
   
   # Get the source table DDL
   ddl.dest <- db.nzget.ddl(tab.src, dsn = dsn.src, rename.to = tab.dest)   
   
   # Create the destination table.
   db.nzquery(ddl.dest, dsn = dsn.dest)
   
   # Temp file name for external table.
   tmp.uid <- db.uid()
   tmp.file <- paste("tmp_nzmigrate_", tmp.uid, sep = "")
   tmp.path.src <- paste(tmpdir.src, tmp.file, sep = "/")
   tmp.path.dest <- paste(tmpdir.dest, tmp.file, sep = "/")
   tmp.ext.tab <- paste(tmp.file, "_ext", sep = "")
   
   # Create an external table in the destination DSN attached to the export 
   db.nzquery("
               create external table TAB.EXT sameas TAB.SRC 
               using (dataobject (PATH.SRC) compress TRUE format 'internal')
               ", var.list = list(TAB.EXT = db.literal(tmp.ext.tab), TAB.SRC = db.literal(tab.src), PATH.SRC = tmp.path.src), 
         verbose = FALSE, dsn = dsn.src)
   
   # Export data into an external table.
   if (verbose) print("Exporting...")
   res <- db.nzquery("insert into TAB.EXT SQL.SRC", 
         var.list = list(TAB.EXT = db.literal(tmp.ext.tab), SQL.SRC = db.literal(query.sql)), 
         verbose = verbose, dsn = dsn.src)   
   
   # Make the file available to the destination
   if (remote.src != remote.dest || tmpdir.src != tmpdir.dest)
   {
      cp.remote(remote.src = remote.src, path.src = tmp.path.src, 
            remote.dest = remote.dest, path.dest = tmp.path.dest, 
            verbose = verbose)
   }
   
   # Create an external table in the destination DSN attached to the export 
   db.nzquery("
               create external table TAB.EXT sameas TAB.DEST 
               using (dataobject (PATH.DEST) compress TRUE format 'internal')
               ", var.list = list(TAB.EXT = db.literal(tmp.ext.tab), TAB.DEST = db.literal(tab.dest), PATH.DEST = tmp.path.dest), 
         verbose = FALSE, dsn = dsn.dest)
   
   # Load the data into the destination table
   db.nzquery("insert into TAB.DEST select * from TAB.EXT", 
      var.list = list(TAB.EXT = db.literal(tmp.ext.tab), TAB.DEST = db.literal(tab.dest)), 
      verbose = verbose, dsn = dsn.dest)            
   
   # Drop the external tables
   db.nzdrop.tables(tmp.ext.tab, dsn = dsn.src)
   db.nzdrop.tables(tmp.ext.tab, dsn = dsn.dest)
   
   # Remove the file(s)
   run.remote(paste("rm -f", tmp.path.src), remote = remote.src)
   if (remote.src != remote.dest || tmpdir.src != tmpdir.dest)
   {
      run.remote(paste("rm -f", tmp.path.dest), remote = remote.dest)
   }
   
}

#-------------------------------------------------------------------------------

