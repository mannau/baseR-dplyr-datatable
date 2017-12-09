library(hdf5r)
library(data.table)
library(lubridate)
library(magrittr)
library(dplyr)

load_data <- function(fname, pairs, folder = c("POLONIEX", "KRAKEN")) {
  folder <- match.arg(folder, several.ok = FALSE)
  f <- h5file(fname, mode = "r")
  on.exit(f$close_all())
  
  out <- list()
  for (pair in pairs) {
    cat(sprintf("*** Processing %s...\n", pair))
    ds <- f[[file.path(pair, "trades")]]
    dat <- ds$read()
    if (folder == "KRAKEN") {
      colnames(dat) <- c("Date", "Price", "Volume", "NA")
    } else if (folder == "POLONIEX") {
      colnames(dat) <- c("Date", "Price", "Volume", "GlobalTradeID", "TradeID", "nchunk")
    } else {
      stop(sprintf("Folder '%s' not supported yet.", folder))
    }
    
    # Clean data
    dat <- dat[dat[, "Date"] > 0, ]
    
    dat <- as.data.frame(dat)
    dat[, "Date"] <- as.POSIXct(dat[, "Date"], origin = "1970-01-01")
    out[[pair]] <- dat
  }
  out
}

calc_index <- function(dat, bname = "min", barsize = 60, firstdt = NULL, lastdt = NULL) {
  
  if (is.null(firstdt)) firstdt <- as.POSIXct(min(dat[, "Date"]), origin = "1970-01-01", tz = "UTC")
  if (is.null(lastdt)) lastdt  <- as.POSIXct(max(dat[, "Date"]), origin = "1970-01-01", tz = "UTC")

  cat(sprintf("Aggregating %s (%d) Bars...\n", bname, barsize))
  
  t0 <- floor_date(firstdt, unit = bname)
  tn <- ceiling_date(lastdt, unit = bname)
  ts <- seq(from = t0, to = tn, by = barsize)
  interval <- findInterval(dat[, "Date"], ts)
  ts[interval]
}

agg_ohcl_data_table <- function(dat, index) {
  dat <- data.table(dat)
  dat[, Index := index]
  setkey(dat, Date) # do the ordering implicitly
  setkey(dat, Index)
  
  dat[, .(Open = data.table::first(Price), 
         High = max(Price), 
         Low = min(Price), 
         Close = data.table::last(Price), 
         Volume = sum(Volume)), by = Index]
}

agg_ohcl_base_r <- function(dat, index) {
  oidx <- order(dat$Date)
  dat <- dat[oidx, ]
  index <- index[oidx]
  
  data.frame(
    Open = tapply(dat$Price, index, first),
    High = tapply(dat$Price, index, max),
    Low = tapply(dat$Price, index, min),
    Close = tapply(dat$Price, index, tail, 1),
    Volume = tapply(dat$Volume, index, sum)
  )
}

agg_ohcl_dplyr <- function(dat, index) {
  dat$Index <- index
  dat %>% 
    arrange(Date) %>%
    group_by(Index) %>%
    summarise(Open = dplyr::first(Price), 
              High = max(Price), 
              Low = min(Price), 
              Close = dplyr::last(Price), 
              Volume = sum(Volume))
}

