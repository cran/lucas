% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/lucas.r
\name{Add_photo_fields_2006}
\alias{Add_photo_fields_2006}
\alias{Add_missing_cols}
\title{Add photo fields 2006}
\usage{
Add_photo_fields_2006(con)

Add_missing_cols(con, years)
}
\arguments{
\item{con}{Connection to database}

\item{years}{Numeric vector of years to be harmonised}
}
\description{
Add photo fields 2006

Add missing columns
}
\section{Functions}{
\itemize{
\item \code{Add_photo_fields_2006}: missing columns photo_n/e/s/w in 2006 data from the information of the exif DB

\item \code{Add_missing_cols}: missing columns to all tables before merge
}}

\examples{
\dontrun{
Add_photo_fields_2006n(con)}
\dontrun{
Add_missing_cols(con, c(2006, 2009, 2012, 2015, 2018))}
}
\seealso{
To create the conection please see \link[lucas]{Connect_to_db}

To create the conection please see \link[lucas]{Connect_to_db}
}
