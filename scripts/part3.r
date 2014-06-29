install.packages("data.table")
install.packages("ggplot2")
library(data.table)
library(ggplot2)

beta_coefs <- data.table(read.csv("beta_coeficients.csv", col.names=list("feature", "beta", "a", "b")))
beta_coefs <- beta_coefs[order(-beta)]

names_top <- paste("feature = ", beta_coefs$feature)
names_top[3:length(names_top)] <- NA
names_bottom <- paste("feature = ", beta_coefs$feature)
names_bottom[1:(length(names_bottom)-4)] <- NA

ggplot(beta_coefs, aes(x=c(1:nrow(beta_coefs)), y=beta)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(beta_coefs))+nrow(beta_coefs)/100, y=beta, label=names_top, hjust=0, vjust=0), size=3) +
 geom_text(aes(x=c(1:nrow(beta_coefs))-nrow(beta_coefs)/100, y=beta, label=names_bottom, hjust=1.0, vjust=0.5), size=3) +
 xlab("features") +
 ylab("beta")

beta_coefs <- beta_coefs[order(-abs(beta))]
beta_coefs[1:10]

