#!/usr/bin/env Rscript

packages.to.install <- c("grid", "ggplot2")

for (p in packages.to.install)
{
    print(p)
    if (suppressWarnings(!require(p, character.only = TRUE))) {
        install.packages(p, repos = "http://lib.stat.cmu.edu/R/CRAN")
        library(p, character.only=TRUE)
    }
}

png(file = "current_bench/summary.png", width = 1200, height = 1200)

dat <- read.csv("current_bench/bench.csv")
init_ts = dat$ts[1]
dat$ts = dat$ts - init_ts
bench_plot <- ggplot(dat, aes(x = ts)) +
                labs(x = "Elapsed Secs", y = "") +
                theme(legend.title=element_blank())

plot1 <- bench_plot + labs(title = "Publisher and Consumer Rates (msgs/sec)") +
            geom_smooth(aes(y = nr_of_pub_msgs, color = "Publisher Rate"), size=0.5) +
            geom_point(aes(y = nr_of_pub_msgs, color = "Publisher Rate"), size=2.0) +

            geom_smooth(aes(y = nr_of_con_msgs, color = "Consumer Rate"), size=0.5) +
            geom_point(aes(y = nr_of_con_msgs, color = "Consumer Rate"), size=2.0)

plot3 <- bench_plot + labs(title = "Nr of Publishers") +
            geom_line(aes(y = nr_of_pubs, color = "Nr of Publishers"))

plot4 <- bench_plot + labs(title = "Nr of Consumers") +
            geom_line(aes(y = nr_of_cons, color = "Nr of Consumers"))

plot5 <- bench_plot + labs(title = "Latencies in ms") +
            geom_line(aes(y = avg_latency, color = "avg"), size=0.5) +
            geom_line(aes(y = median_latency, color = "median"), size=0.5) +
            geom_line(aes(y = lat_50, color = "50%"), size=0.5) +
            geom_line(aes(y = lat_75, color = "75%"), size=0.5) +
            geom_line(aes(y = lat_95, color = "95%"), size=0.5) +
            geom_line(aes(y = lat_99, color = "99%"), size=0.5) +
            geom_line(aes(y = lat_999, color = "99.9%"), size=0.5) +

pushViewport(viewport(layout = grid.layout(4, 1, heights=c(1.5, 1.5, 0.5, 0.5))))
vplayout <- function(x,y) viewport(layout.pos.row = x, layout.pos.col = y)

print(plot1, vp=vplayout(1,1))
print(plot5, vp=vplayout(2,1))
print(plot3, vp=vplayout(3,1))
print(plot4, vp=vplayout(4,1))

png(file = "current_bench/pubsub.png", width = 1200, height = 400)
print(plot1)
png(file = "current_bench/latencies.png", width = 1200, height = 400)
print(plot5)

dev.off()
