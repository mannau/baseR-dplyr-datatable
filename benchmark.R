library(microbenchmark)
library(ggplot2)

source("functions.R")
mostliquid <- get_most_liquid()

# dat.all.dt.nokey <- lapply(rownames(mostliquid), function(x) {
#   dat <- load_data("data/poloniex.h5", x)[[1]]
#   index <- calc_index(dat)
#   out <- agg_ohcl_base_r(dat, index)[, c("Index", "Close")]
#   setnames(out,"Close", x)
#   out
# })
# 
# saveRDS(dat.all.dt.nokey, "data/dat_all_df.rds")

# 1.

dat <- load_data("data/poloniex.h5", "BTC_XRP")[[1]]
index <- calc_index(dat)

mb1 <- microbenchmark(
  base_r_tapply = agg_ohcl_base_r_tapply(dat, index),
  base_r_split = agg_ohcl_base_r_split(dat, index),
  base_r_match = agg_ohcl_base_r_match(dat, index),
  #agg_ohcl_base_r_aggregate(dat, index), # removed since imho redundant with agg_ohcl_base_r_split
  dplyr = agg_ohcl_dplyr(dat, index), 
  data_table_noGForce = agg_ohcl_data_table_noGForce(dat, index, usekey = TRUE),
  data_table_nokey = agg_ohcl_data_table(dat, index, usekey = FALSE),
  data_table_key = agg_ohcl_data_table(dat, index, usekey = TRUE),
  times = 5)

saveRDS(mb1, "data/bench_r_1.rds")

# 2.

dat.all.df <- readRDS("data/dat_all_df.rds")
dat.all.dt.nokey <- lapply(dat.all.df, function(x) data.table(x))
dat.all.dt.key <- lapply(dat.all.dt.nokey, function(x) {setkey(x, "Index");x})

## full outer join
mb2 <- microbenchmark(
         base_r_merge = Reduce(function(...) base::merge(..., by = "Index", all = TRUE), dat.all.df), 
         dplyr_full_join = purrr::reduce(dat.all.df, function(x, y) full_join(x, y, by = "Index")),
         data_table_nokey_merge = Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.dt.nokey), 
         data_table_key_merge = Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.dt.key), 
         times = 5)

saveRDS(mb2, "data/bench_r_2.rds")

# 3.
base_r_merge = Reduce(function(...) base::merge(..., by = "Index", all = TRUE), dat.all.df)
dplyr_full_join = purrr::reduce(dat.all.df, function(x, y) full_join(x, y, by = "Index"))
data_table_nokey_merge = Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.dt.nokey)
data_table_key_merge = Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.dt.key)

## filter
mb3 <- microbenchmark(
  base_r_df = base_r_merge[base_r_merge$Index > as.POSIXct("2016-12-12") & base_r_merge$Index < as.POSIXct("2016-12-31"), ],
  dbplyr_filter = dplyr_full_join %>% filter(between(Index, as.POSIXct("2016-12-12"), as.POSIXct("2016-12-31"))),
  data_table_nokey = data_table_nokey_merge[Index > as.POSIXct("2016-12-12") & Index < as.POSIXct("2016-12-31")],
  data_table_key = data_table_key_merge[Index > as.POSIXct("2016-12-12") & Index < as.POSIXct("2016-12-31")],
  times = 10)

saveRDS(mb3, "data/bench_r_3.rds")



