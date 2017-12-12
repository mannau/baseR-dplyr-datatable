
library("microbenchmark")
source("functions.R")

btcxrp <- load_data("data/poloniex.h5", "BTC_XRP", folder = "POLONIEX")[[1]]
index <- calc_index(btcxrp)

tmp <- rename(btcxrp, TStamp = Date) %>%
       mutate(Idx = index)

##### MonetDBLite
# https://github.com/hannesmuehleisen/MonetDBLite-R
# https://github.com/szilard/benchm-databases

# prepare table
library("dplyr")
library("DBI")
dbdir <- tempdir()
monetdb_con <- dbConnect(MonetDBLite::MonetDBLite())
dbWriteTable(monetdb_con, "btcxrp", tmp, overwrite=TRUE)

# dplyr SQL tbl
monetdb_src <- MonetDBLite::src_monetdblite(dbdir)
monetdb_tbl <- tbl(monetdb_src, "btcxrp")

##### SQLite (in-memory)

# prepare table
library("dplyr")
library("RSQLite")
sqlite_con <- dbConnect(dbDriver("SQLite"), dbname = ":memory:")
dbWriteTable(sqlite_con, "btcxrp", tmp)

# dplyr SQL tbl
sqlite_tbl <- tbl(sqlite_con, "btcxrp")

##### aggregation benchmark

agg_dplyr <- function(dat) {
  dat %>%
  arrange(TStamp) %>%
  group_by(Idx) %>%
  summarise(High = max(Price), Low = min(Price), Volume = sum(Volume))
}

agg_dplyr(monetdb_tbl) %>% show_query()
agg_dplyr(sqlite_tbl) %>% show_query()

mb <- microbenchmark(
  res_df <- agg_dplyr(tmp),
  res_monetdb <- agg_dplyr(monetdb_tbl) %>% collect(),
  res_sqlite <- agg_dplyr(sqlite_tbl) %>% collect(),
  times = 1)
mb

dbDisconnect(sqlite_con)
dbDisconnect(monetdb_con)
