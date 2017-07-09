p2 <- read.csv("p2.csv", sep=",", header=TRUE)
png('p2.png')
K <- p2$K
Avg_Distance <- p2$Avg_Distance
plot(K, Avg_Distance, main="K vs Avg_Distance",type="l")
dev.off()