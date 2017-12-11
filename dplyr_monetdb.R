
library("microbenchmark")
source("functions.R")

btcxrp <- load_data("data/poloniex.h5", "BTC_XRP", folder = "POLONIEX")[[1]]
index <- calc_index(btcxrp)

tmp <- rename(btcxrp, TStamp = Date) %>%
       mutate(Idx = index)

library("DBI")
dbdir <- tempdir()
con <- dbConnect(MonetDBLite::MonetDBLite())
dbWriteTable(con, "btcxrp", tmp, overwrite=TRUE)

library("dplyr")
ms <- MonetDBLite::src_monetdblite(dbdir)
mt <- tbl(ms, "btcxrp")

agg_dplyr <- function(dat) {
  dat %>%
  arrange(TStamp) %>%
  group_by(Idx) %>%
  summarise(High = max(Price), Low = min(Price), Volume = sum(Volume))
}

mb <- microbenchmark(
  res1 <- agg_dplyr(tmp),
  res2 <- agg_dplyr(mt) %>% collect(),
  times = 5)
mb
