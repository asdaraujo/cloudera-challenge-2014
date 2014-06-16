# avg_charged_amount by provider_id

p <- data.table(read.csv("procedures.csv",
  header=FALSE,
  col.names=c("type", "procedure_id", "provider_id", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"),
  colClasses=c("factor", "factor", "numeric", "factor", "factor", "factor", "factor", "numeric", "numeric", "numeric")
))

setkey(p, procedure_id, provider_id)
a<-p[,list(mean_total_charges=mean(total_charges),sd_total_charges=sd(total_charges),mean_avg_charged_amount=mean(avg_charged_amount),sd_avg_charged_amount=sd(avg_charged_amount),mean_avg_total_payment=mean(avg_total_payment),sd_avg_total_payment=sd(avg_total_payment),mean_total_amount=mean(total_charges*avg_charged_amount),sd_total_amount=sd(total_charges*avg_charged_amount)),by=list(procedure_id)]
b<-p[,list(count=.N, type, provider_city, provider_state, total_charges=sum(total_charges), avg_charged_amount=sum(total_charges*avg_charged_amount)/sum(total_charges), avg_total_payment=sum(total_charges*avg_total_payment)/sum(total_charges)),by=list(procedure_id, provider_id, type, provider_city, provider_state)]
setkey(a, procedure_id)
setkey(b, procedure_id)
c<-a[b][,list(type, provider_city, provider_state, procedure_id, provider_id, count, total_charges, avg_charged_amount, total_amount=total_charges*avg_charged_amount, z=(avg_charged_amount-mean_avg_charged_amount)/sd_avg_charged_amount)]

c<-c[order(-z)]
names<-paste("provider_id = ", c$provider_id)
names[4:length(names)]<-NA

ggplot(c, aes(x=c(1:nrow(c)), y=z)) + 
geom_point(shape=19, size=1, alpha=1/4) + 
geom_text(aes(x=c(1:nrow(c))+nrow(c)/100, y=z, label=names), size=3, hjust=0)


# avg_charged_amount by state,city

p <- data.table(read.csv("procedures.csv",
  header=FALSE,
  col.names=c("type", "procedure_id", "provider_id", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"),
  colClasses=c("factor", "factor", "numeric", "factor", "factor", "factor", "factor", "numeric", "numeric", "numeric")
))

setkey(p, procedure_id, provider_city, provider_state)
a<-p[,list(procedure_id,mean_total_charges=mean(total_charges),sd_total_charges=sd(total_charges),mean_avg_charged_amount=mean(avg_charged_amount),sd_avg_charged_amount=sd(avg_charged_amount),mean_avg_total_payment=mean(avg_total_payment),sd_avg_total_payment=sd(avg_total_payment)),by=list(procedure_id)]
b<-p[,list(count=.N, total_charges=sum(total_charges), avg_charged_amount=sum(total_charges*avg_charged_amount)/sum(total_charges), avg_total_payment=sum(total_charges*avg_total_payment)/sum(total_charges)),by=list(procedure_id, provider_city, provider_state, type)]
setkey(a, procedure_id)
setkey(b, procedure_id)
c<-b[a,list(type, provider_city, provider_state, count, z=(avg_charged_amount-mean_avg_charged_amount)/sd_avg_charged_amount)]

c<-c[order(-z)]
names<-paste("city,state = ", c$provider_city, ",", c$provider_state)
names[4:length(names)]<-NA

ggplot(c, aes(x=c(1:nrow(c)), y=z)) +
geom_point(shape=19, size=1, alpha=1/4) + 
geom_text(aes(x=c(1:nrow(c))+nrow(c)/100, y=z, label=names), size=3, hjust=0)


# avg_charged_amount by referral region

p <- data.table(read.csv("procedures.csv",
  header=FALSE,
  col.names=c("type", "procedure_id", "provider_id", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"),
  colClasses=c("factor", "factor", "numeric", "factor", "factor", "factor", "factor", "numeric", "numeric", "numeric")
))

setkey(p, procedure_id, provider_city, provider_state)
a<-p[,list(procedure_id,mean_total_charges=mean(total_charges),sd_total_charges=sd(total_charges),mean_avg_charged_amount=mean(avg_charged_amount),sd_avg_charged_amount=sd(avg_charged_amount),mean_avg_total_payment=mean(avg_total_payment),sd_avg_total_payment=sd(avg_total_payment)),by=list(procedure_id)]
b<-p[,list(count=.N, r=nrow(.SD), total_charges=sum(total_charges), avg_charged_amount=sum(total_charges*avg_charged_amount)/sum(total_charges), avg_total_payment=sum(total_charges*avg_total_payment)/sum(total_charges)),by=list(procedure_id, referral_region, type)]
setkey(a, procedure_id)
setkey(b, procedure_id)
c<-b[a,list(type, referral_region, city=substr(referral_region,6,100), state=substr(referral_region,1,2), count, z=(avg_charged_amount-mean_avg_charged_amount)/sd_avg_charged_amount)]

c<-c[order(-z)]
names<-paste("ref city,state = ", c$city, ",", c$state)
names[4:length(names)]<-NA

ggplot(c, aes(x=c(1:nrow(c)), y=z)) +
geom_point(shape=19, size=1, alpha=1/4) + 
geom_text(aes(x=c(1:nrow(c))+nrow(c)/100, y=z, label=names), size=3, hjust=0)



# Charges to population ratio

p <- data.table(read.csv("procedures.csv",
  header=FALSE,
  col.names=c("type", "procedure_id", "provider_id", "provider_city", "provider_state", "provider_zipcode", "referral_region", "total_charges", "avg_charged_amount", "avg_total_payment"),
  colClasses=c("factor", "factor", "numeric", "factor", "factor", "factor", "factor", "numeric", "numeric", "numeric")
))

q<-data.table(read.csv("us_cities_pop.csv", col.names=c("state","city","population"), colClasses=c("factor","factor","numeric"), na.strings=c("X")))
setkey(q, state, city)
pop<-q[,list(population=sum(population)),by=list(state, city)]

setkey(p, provider_state, provider_city)
setkey(pop, state, city)
p<-pop[p]

setkey(p, procedure_id, provider_id)
a<-p[!is.na(population),list(mean_total_charges=mean(total_charges),sd_total_charges=sd(total_charges),mean_avg_charged_amount=mean(avg_charged_amount),sd_avg_charged_amount=sd(avg_charged_amount),mean_avg_total_payment=mean(avg_total_payment),sd_avg_total_payment=sd(avg_total_payment),mean_total_amount=mean(total_charges*avg_charged_amount),sd_total_amount=sd(total_charges*avg_charged_amount),mean_relative_charges=mean(total_charges/population),sd_relative_charges=sd(total_charges/population)),by=list(procedure_id)]
b<-p[!is.na(population),list(count=.N, population=max(population), total_charges=sum(total_charges), relative_charges=sum(total_charges)/max(population), avg_charged_amount=sum(total_charges*avg_charged_amount)/sum(total_charges), avg_total_payment=sum(total_charges*avg_total_payment)/sum(total_charges)),by=list(procedure_id, provider_id)]
c<-a[b][,list(procedure_id, provider_id, count, total_charges, population, relative_charges, mean_relative_charges, avg_charged_amount, total_amount=total_charges*avg_charged_amount, z=(relative_charges-mean_relative_charges)/sd_relative_charges)]

c[order(z)]

ggplot(c, aes(x=procedure_id, y=z)) + geom_point(shape=19, size=1, alpha=1/4)

