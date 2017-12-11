library("microbenchmark")
library("dplyr")
library("purrr")

source("functions.R")

mostliquid <- get_most_liquid()

# 1.

agg_bench <- function(pairs, fname = "data/poloniex.h5", FUN = agg_ohcl_dplyr, ...) {
  sapply(pairs, function(x) {
    time.load <- system.time(dat <- load_data("data/poloniex.h5", x)[[1]])["elapsed"]
    time.idx <- system.time(index <- calc_index(dat))["elapsed"]
    time.agg <- system.time(FUN(dat, index, ...))["elapsed"]
    c(load = time.load, index = time.idx, agg = time.agg)
  })
}

out1 <- agg_bench(rownames(mostliquid), FUN = agg_ohcl_dplyr)
bench_dplyr <- data.frame(Method = "dplyr", melt(out1))

saveRDS(bench_dplyr, file = "data/bench_dplyr_1.rds")


# 2.

dat.all.dplyr <- lapply(rownames(mostliquid), function(x) {
  dat <- load_data("data/poloniex.h5", x)[[1]]
  index <- calc_index(dat)
  agg_ohcl_dplyr(dat, index) %>%
    select(Index, Close) %>%
    set_names(c("Index", x))
})

## full outer join
mb <- microbenchmark(reduce(dat.all.dplyr,
                            function(x, y) full_join(x, y, by = "Index") %>%
                                           arrange(Index)),
                     times = 10)

