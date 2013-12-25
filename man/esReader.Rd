\name{esReader}
\alias{esReader}
\alias{esReader}
\docType{package}
\title{
Provides an ElasticSearch Reader for ProjectTemplate
}
\description{
Creates an ElasticSearch Reader for ProjectTemplate. Install and include es-reader as
a required package. Reads from a *.es file in the data folder:
}
\author{
Kenton White

Maintainer: Kenton White <jkentonwhite@gmail.com>
}
\details{
Example of the DCF format and settings used in a .es file are shown
below:

host: localhost
port: 9200
index: sample_index
field: text
query: this AND that
}
