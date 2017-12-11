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

out1 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_data_table, hashkey = TRUE)
out2 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_base_r)
out3 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_base_r_match)
out4 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_base_r_aggregate)
out5 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_dplyr)

bench_r_1 <- rbind(
  data.frame(Method = "data.table", melt(out1)),
  data.frame(Method = "duplicated", melt(out2)),
  data.frame(Method = "match", melt(out3)),
  data.frame(Method = "aggregate", melt(out4)),
  data.frame(Method = "dplyr", melt(out5))
)

save(bench_r_1, file = "data/bench_r_1.rda")

##head(bench_r_1)
##ggplot(bench_r_1) + geom_bar(aes(x = Method, y = value, fill = Var1), stat = "sum")

# 2.

dat.all.hashkey <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  out <- agg_ohcl_base_r(dat, index)[, c("Index", "Close")]
  setnames(out,"Close", x)
  out
})

dat.all.nohashkey <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  out <- agg_ohcl_data_table(dat, index)[, c("Index", "Close")]
  setnames(out,"Close", x)
  out
})

## full outer join
mb <- microbenchmark(Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.hashkey), 
               Reduce(function(...) merge(..., by = "Index", all = TRUE), dat.all.nohashkey), times = 10)

mb$expr <- c("key=TRUE", "key=FALSE")
save(bench_r_2, file = "data/bench_r_2.rda")

# 3.

# 2.
dat.all <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  out <- agg_ohcl_data_table(dat, index, hashkey = TRUE)[, c("Index", "Close")]
  setnames(out,"Close", x)
  out
})

tmp <- lapply(dat.all, head)
args(Reduce)
args(merge)
?Reduce
head(Reduce(function(x, y) base::merge(x, y, all = TRUE), tmp))

head(dat.all[[1L]])
head(dat.all[[2L]])

## full outer join
mb <- microbenchmark(z <- Reduce(function(x, y) base::merge(x,, by = "Index", all = TRUE), dat.all), times = 10)
plot(mb)

