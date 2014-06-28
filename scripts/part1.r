install.packages("data.table")
install.packages("ggplot2")
library(data.table)
library(ggplot2)

inp <- read.csv("Medicare_Provider_Charge_Inpatient_DRG100_FY2011.csv", col.names=list("procedure_id", "provider_id", "provider_name", "provider_street", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"))
oup <- read.csv("Medicare_Provider_Charge_Outpatient_APC30_CY2011_v2.csv", col.names=list("procedure_id", "provider_id", "provider_name", "provider_street", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"))
inp <- cbind(inp, type="inpatient")
oup <- cbind(oup, type="outpatient")
all <- data.table(rbind(inp, oup))
rm(inp)
rm(oup)

# part 1a

setkey(all, procedure_id)
rel_var <- all[,list(rv=(sd(avg_charged_amount)/mean(avg_charged_amount))^2),by=list(procedure_id)]
rel_var <- rel_var[order(-rv)]

rel_var[1:3]

names<-paste("procedure_id = ", rel_var$procedure_id)
names[8:length(names)]<-NA
ggplot(rel_var, aes(x=c(1:nrow(rel_var)), y=rv)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(rel_var))+nrow(rel_var)/100, y=rv, label=names), size=3, hjust=0) +
 xlab("procedures") +
 ylab("relative variance")

# part 1b

setkey(all, procedure_id, provider_id)
amount <- all[,list(avg_charged_amount),by=list(procedure_id, provider_id)]
amount <- amount[order(procedure_id, -avg_charged_amount)]
highest_per_proc <- amount[,list(provider_id=.SD[1,provider_id]),by=list(procedure_id)]

setkey(highest_per_proc, provider_id)
high_prov_count <- highest_per_proc[,list(count=.N),by=list(provider_id)]
high_prov_count <- high_prov_count[order(-count)]

high_prov_count[1:3]

names<-paste("provider_id = ", high_prov_count$provider_id)
names[4:length(names)]<-NA
ggplot(high_prov_count, aes(x=c(1:nrow(high_prov_count)), y=count)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(high_prov_count))+nrow(high_prov_count)/100, y=count, label=names), size=3, hjust=0) +
 xlab("providers") +
 ylab("# of highest claims")

# part 1c

setkey(all, procedure_id, referral_region)
amount <- all[,list(avg_per_region=sum(total_charges*avg_charged_amount)/sum(total_charges)),by=list(procedure_id, referral_region)]
amount <- amount[order(procedure_id, -avg_per_region)]
highest_per_proc <- amount[,list(referral_region=.SD[1,referral_region]),by=list(procedure_id)]

setkey(highest_per_proc, referral_region)
region_count <- highest_per_proc[,list(count=.N),by=list(referral_region)]
region_count <- region_count[order(-count)]

region_count[1:3]

names<-paste("region = ", region_count$referral_region)
names[4:length(names)]<-NA
ggplot(region_count, aes(x=c(1:nrow(region_count)), y=count)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(region_count))+nrow(region_count)/100, y=count, label=names), size=3, hjust=0) +
 xlab("referral regions") +
 ylab("# of highest claims")

# part 1d

setkey(all, procedure_id, provider_id)
amount <- all[,list(claim_diff=avg_charged_amount-avg_total_payment),by=list(procedure_id, provider_id)]
amount <- amount[order(procedure_id, -claim_diff)]
highest_per_proc <- amount[,list(provider_id=.SD[1,provider_id]),by=list(procedure_id)]

setkey(highest_per_proc, provider_id)
high_prov_count <- highest_per_proc[,list(count=.N),by=list(provider_id)]
high_prov_count <- high_prov_count[order(-count)]

high_prov_count[1:3]

names<-paste("provider_id = ", high_prov_count$provider_id)
names[4:length(names)]<-NA
ggplot(high_prov_count, aes(x=c(1:nrow(high_prov_count)), y=count)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(high_prov_count))+nrow(high_prov_count)/100, y=count, label=names), size=3, hjust=0) +
 xlab("providers") +
 ylab("# of highest claim differences")

