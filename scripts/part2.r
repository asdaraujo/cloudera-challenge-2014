inp <- read.csv("Medicare_Provider_Charge_Inpatient_DRG100_FY2011.csv", col.names=list("procedure_id", "provider_id", "provider_name", "provider_street", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"))
oup <- read.csv("Medicare_Provider_Charge_Outpatient_APC30_CY2011_v2.csv", col.names=list("procedure_id", "provider_id", "provider_name", "provider_street", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"))
inp <- cbind(inp, type="inpatient")
oup <- cbind(oup, type="outpatient")
all <- data.table(rbind(inp, oup))

setkey(all, procedure_id, provider_id)
proc_stats <- all[,list(mean_avg_charged_amount=mean(avg_charged_amount),sd_avg_charged_amount=sd(avg_charged_amount)),by=list(procedure_id)]
setkey(proc_stats, procedure_id)
norm_all <- proc_stats[all][,list(type, referral_region, provider_id, z=(avg_charged_amount-mean_avg_charged_amount)/sd_avg_charged_amount)]

prov_stats <- norm_all[,list(count=.N, var_z=sd(z)),by=list(provider_id, type)]
prov_stats <- prov_stats[order(-var_z)]
prov_stats

names <- paste("provider = ", prov_stats$provider_id)
names[4:length(names)] <- NA
ggplot(prov_stats, aes(x=c(1:nrow(prov_stats)), y=var_z)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(prov_stats))+nrow(prov_stats)/100, y=var_z, label=names), size=3, hjust=0) +
 xlab("providers") +
 ylab("variance of standard score of claim amounts")


region_stats <- norm_all[,list(count=.N, var_z=var(z)),by=list(referral_region, type)]
region_stats <- region_stats[order(-var_z)]
region_stats

names <- paste("region = ", region_stats$referral_region)
names[4:length(names)] <- NA
ggplot(region_stats, aes(x=c(1:nrow(region_stats)), y=var_z)) +
 geom_point(shape=19, size=2, alpha=1/4) +
 geom_text(aes(x=c(1:nrow(region_stats))+nrow(region_stats)/100, y=var_z, label=names), size=3, hjust=0) +
 xlab("regions") +
 ylab("variance of standard score of claim amounts")

