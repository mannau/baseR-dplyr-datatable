# Download data into /data folder

webpath <- "https://s3.eu-central-1.amazonaws.com/vienna-r-blog/assets/cryptocoins"
targetpath <- "data"
file.create(targetpath, recursive = TRUE, showWarnings = FALSE)
files.source <- file.path(webpath, c("kraken.h5", "poloniex.h5"))
files.target <- file.path(targetpath, c("kraken.h5", "poloniex.h5"))
download.file(files.source, destfile = files.target)
