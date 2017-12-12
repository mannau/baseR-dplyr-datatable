library(hdf5r)
library(data.table)
library(lubridate)
library(magrittr)
library(dplyr)

get_most_liquid <- function() {
  f <- h5file("data/poloniex.h5", "r")
  on.exit(f$close_all())
  
  mostliquid <- sort(sapply(list.groups(f), function(x) f[[sprintf("%s/trades", x)]]$dims[1] ), decreasing = TRUE)[1:11]
  #mostliquid <- round(mostliquid / 1e6, 2)
  crosses <- sub("_", "/", names(mostliquid))
  crosses <- sub("USDT/BTC", "BTC/USD", crosses)
  mostliquid <- data.frame(CCY = crosses, TICKS = mostliquid)
  mostliquid$Cross <- factor(ifelse(mostliquid$CCY == "BTC/USD", "USD-Cross", "BTC-Cross"))
  exceptidx <- mostliquid$CCY != "BTC/USD"
  mostliquid$CCY <- factor(mostliquid$CCY, levels = c(as.character(mostliquid$CCY[exceptidx]), as.character(mostliquid$CCY[!exceptidx])))
  mostliquid
}

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

agg_ohcl_data_table <- function(dat, index, usekey = TRUE) {
  dat <- data.table(dat)
  dat[, Index := index]
  
  if (usekey) {
    setkey(dat, Date) # do the ordering implicitly
    setkey(dat, Index)
  } else {
    dat <- dat[order(Date),]
  }
  
  dat[, .(Open = first(Price), 
         High = max(Price), 
         Low = min(Price), 
         Close = last(Price), 
         Volume = sum(Volume)), by = Index]
}

agg_ohcl_data_table_noGForce <- function(dat, index, usekey = TRUE) {
  dat <- data.table(dat)
  dat[, Index := index]
  
  if (usekey) {
    setkey(dat, Date) # do the ordering implicitly
    setkey(dat, Index)
  } else {
    dat <- dat[order(Date),]
  }
  
  dat[, .(Open = data.table::first(Price), 
          High = max(Price), 
          Low = min(Price), 
          Close = data.table::last(Price), 
          Volume = sum(Volume)), by = Index]
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


agg_ohcl_base_r_tapply <- function(dat, index) {
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

agg_ohcl_base_r_aggregate <- function(dat, index) {
    oidx <- order(dat$Date)
    dat <- dat[oidx, ]
    index <- index[oidx]
    x <- aggregate(dat[, c("Price", "Volume")], by = list(index), FUN = c,
                   simplify = FALSE)
    data.frame(
        Index  = x[,1],
        Open   = as.double(lapply(x$Price, head, 1)),
        High   = as.double(lapply(x$Price, max)),
        Low    = as.double(lapply(x$Price, min)),
        Close  = as.double(lapply(x$Price, tail, 1)),
        Volume = as.double(lapply(x$Volume, sum)))
}

agg_ohcl_base_r_match <- function(dat, index) {
    oidx <- order(dat$Date)
    dat <- dat[oidx, ]
    index <- index[oidx]

    ui <- unique(index)
    j <- match(index, ui)
    dat <- split(dat, j)

    data.frame(
        Index  = ui,
        Open   = as.double(lapply(dat, function(x) head(x$Price, 1))),
        High   = as.double(lapply(dat, function(x) max(x$Price))),
        Low    = as.double(lapply(dat, function(x) min(x$Price))),
        Close  = as.double(lapply(dat, function(x) tail(x$Price, 1))),
        Volume = as.double(lapply(dat, function(x) sum(x$Volume))))
}

agg_ohcl_base_r_split <- function(dat, index) {
    oidx <- order(dat$Date)
    dat <- dat[oidx, c("Price", "Volume")]
    index <- index[oidx]
    ui <- !sorted_duplicated(index)
    i <- cumsum(ui)
    dat <- split(dat, i)
    data.frame(
        Index  = index[ui],
        Open   = as.double(lapply(dat, function(x) head(x$Price, 1))),
        High   = as.double(lapply(dat, function(x) max(x$Price))),
        Low    = as.double(lapply(dat, function(x) min(x$Price))),
        Close  = as.double(lapply(dat, function(x) tail(x$Price, 1))),
        Volume = as.double(lapply(dat, function(x) sum(x$Volume))))
}

sorted_duplicated <- function(x) {
    c(FALSE, tail(x, -1) == head(x, -1))
}

sqlite_agg <- function(dat, index) {
  oidx <- order(dat$Date)
  dat <- dat[oidx, ]
  dat$Minutes <- index[oidx]

  ## dbSendQuery(db, "DROP TABLE tmp;")
  db <- dbConnect(dbDriver("SQLite"), dbname = ":memory:")
  dbWriteTable(db, "tmp", dat)

  query <- paste("SELECT Minutes as Ind, MAX(Price) as High,",
                 "MIN(Price) as Low, SUM(Volume) as Volume", 
                 "FROM tmp GROUP BY Minutes;")
  df <- dbGetQuery(db, query)
  dbDisconnect(db)
  df
}
