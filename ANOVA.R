
library(agricolae)
library(reshape2)

df <- read.csv("Input\\KPI.csv")
df$day <- NULL


col_names <- colnames(df)
col_names <- col_names[col_names != "Scenario_ID"]

y_group_all = list()
i <- 1

for (col_name in col_names) {
  amod <- aov(as.formula(sprintf("%s ~ Scenario_ID", paste(col_name, collapse = " + "))), data=df)
  y <- HSD.test(amod, "Scenario_ID", group=TRUE)
  y_group <- y$groups
  y_group <- y$groups
  y_group <- y_group[order(y_group[,1]),]
  y_group <- data.frame(y_group)
  y_group$Scenario_ID <- row.names(y_group)
  row.names(y_group) <- NULL
  y_group <- melt(data = y_group, id.vars = c("Scenario_ID", 'groups'))
  y_group_all[[i]] <- y_group
  i <- i + 1
  
}

y_group_all = do.call(rbind, y_group_all)

write.csv(y_group_all, 'Output\\ANOVA-posthoc.csv') 