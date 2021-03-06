% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lucas.r
\name{Rename_cols}
\alias{Rename_cols}
\title{Rename columns to match 2018 survey}
\usage{
Rename_cols(con, csv)
}
\arguments{
\item{con}{Connection the database}

\item{csv}{CSV file with the relevant column name mappings}
}
\description{
Columns with different names between the surveys must be made to fit the last survey before merge
}
\examples{
\dontrun{
Rename_cols(con, '/data/LUCAS_harmo/data/mappings/columnRename.csv')}
}
\seealso{
To create the conection please see \link[lucas]{Connect_to_db}
}
