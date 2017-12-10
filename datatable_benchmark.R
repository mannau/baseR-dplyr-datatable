library(microbenchmark)
library(ggplot2)

source("functions.R")

mostliquid <- get_most_liquid()

# 1.

agg_bench <- function(pairs, fname = "data/poloniex.h5", FUN = agg_ohcl_data_table, ...) {
  sapply(pairs, function(x) {
    time.load <- system.time(dat <- load_data("data/poloniex.h5", x)[[1]])[3]
    time.idx <- system.time(index <- calc_index(dat))[3]
    time.agg <- system.time(FUN(dat, index, ...))[3]
    c(load = time.load, index = time.idx, agg = time.agg)
  })
}

out1 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_data_table, hashkey = FALSE)
out2 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_data_table, hashkey = TRUE)
out3 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_data_table_noGForce, hashkey = TRUE)

bench_datatable_1 <- rbind(
  data.frame(Method = "hashkey=FALSE", melt(out1)),
  data.frame(Method = "hashkey=TRUE", melt(out2)),
  data.frame(Method = "no GForce", melt(out3))
)

save(bench_datatable_1, file = "data/bench_datatable_1.rda")

# 2.

dat.all.hashkey <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  out <- agg_ohcl_data_table(dat, index, hashkey = TRUE)[, c("Index", "Close")]
  setnames(out,"Close", x)
  out
})

dat.all.nohashkey <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  out <- agg_ohcl_data_table(dat, index, hashkey = FALSE)[, c("Index", "Close")]
  setnames(out,"Close", x)
  out
})

## full outer join
mb <- microbenchmark(Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.hashkey), 
               Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.nohashkey), times = 10)

mb$expr <- c("key=TRUE", "key=FALSE")
save(bench_datatable_2, file = "data/bench_datatable_2.rda")

# 3.



