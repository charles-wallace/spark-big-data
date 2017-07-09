p1 <- read.csv("p1.csv", sep=",", header=TRUE)
png('p1.png')
K <- p1$K
Avg_Distance <- p1$Avg_Distance
plot(K, Avg_Distance,main="K vs Avg_Distance",type="l") 
dev.off()