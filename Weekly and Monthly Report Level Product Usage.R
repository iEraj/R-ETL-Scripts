
#ETL script to wrangle with different data sources, clean, filter, and join the data for Product Usage reporting.
#Output files, CSV files, from this script will be feeding the Power BI Dashboards for fast and streamlined approach.

######################################################### Weekly Data ####################################################################


library(lubridate)
library(tidyverse)
library(stringr)
library(dplyr)
library(readxl)
library(stringr)
library(openxlsx)
library(writexl)
library(readr)

library(data.table)

#Import the CPT Reports data
wcpt_reports <- read_excel("C:/Desktop/CPT Usage by Users.xlsx", sheet = "WeeklyCPTData")
View(wcpt_reports)

#Import the User List data
User_Lists <- read_excel("C:/Desktop/User Lists.xlsx", sheet = "ADUSA", col_types = c("text", "text", "text", "text", "text"))
View(User_Lists)
glimpse(User_Lists)

#Joining the data on Unify ID
joined_wcpt_data <- inner_join(wcpt_reports, User_Lists, 
                            by = c("User" = "Unify ID"))
View(joined_wcpt_data)

#Creating a CPT_Process Column to group by CRP, BPR, and VC
joined_wcpt_data$CPT_Process <- ifelse(grepl('\\[CRP]', toupper(joined_wcpt_data$Reports)), "CRP", 
                                      ifelse(grepl('\\[PBR]', toupper(joined_wcpt_data$Reports)), "PBR", 
                                             ifelse(grepl('\\[VC]', toupper(joined_wcpt_data$Reports)), "VC", "CPT 2.0")))
View(joined_wcpt_data)

#Filtering the Gateway CPT data
gateway_wcpt <- select(filter(joined_wcpt_data, Department == "Gateway"), -Brand)
View(gateway_wcpt)

#Exporting Gateway CPT data
write.xlsx(gateway_wcpt, "C:/Desktop/R_Weekly_gateway_cpt.xlsx")
write.csv(gateway_wcpt, "C:/Desktop/R_Weekly_gateway_cpt.csv", row.names = FALSE)

#Filtering the ADUSA Brands CPT data
adusa_brands_wcpt <- select(filter(joined_wcpt_data, Department != "Gateway" | is.na(Department)), -Manufacturer)
View(adusa_brands_wcpt)

#Exporting Gateway CPT data
write.xlsx(adusa_brands_wcpt, "C:/Desktop/R_Weekly_adusa_brands_cpt.xlsx")
write.csv(adusa_brands_wcpt, "C:/Desktop/R_Weekly_adusa_brands_cpt.csv", row.names = FALSE)


######################################################### Monthly Data ####################################################################


#Import the CPT Reports data
mcpt_reports <- read_excel("C:/Desktop/CPT Usage by Users.xlsx", sheet = "MonthlyCPTData")
View(mcpt_reports)

#Import the User List data
User_Lists <- read_excel("C:/Desktop/User Lists.xlsx", sheet = "ADUSA", col_types = c("text", "text", "text", "text", "text"))
View(User_Lists)
glimpse(User_Lists)

#Joining the data on Unify ID
joined_mcpt_data <- inner_join(mcpt_reports, User_Lists, 
                              by = c("User" = "Unify ID"))
View(joined_mcpt_data)

#Creating a CPT_Process Column to group by CRP, BPR, and VC
joined_mcpt_data$CPT_Process <- ifelse(grepl('\\[CRP]', toupper(joined_mcpt_data$Reports)), "CRP", 
                                      ifelse(grepl('\\[PBR]', toupper(joined_mcpt_data$Reports)), "PBR", 
                                             ifelse(grepl('\\[VC]', toupper(joined_mcpt_data$Reports)), "VC", "CPT 2.0")))
View(joined_mcpt_data)

#Filtering the Gateway CPT data
gateway_mcpt <- select(filter(joined_mcpt_data, Department == "Gateway"), -Brand)
View(gateway_mcpt)

#Exporting Gateway CPT data
write.xlsx(gateway_mcpt, "C:/Desktop/R_Monthly_gateway_cpt.xlsx")
write.csv(gateway_mcpt, "C:/Desktop/R_Monthly_gateway_cpt.csv", row.names = FALSE)

#Filtering the ADUSA Brands CPT data
adusa_brands_mcpt <- select(filter(joined_mcpt_data, Department != "Gateway" | is.na(Department)), -Manufacturer)
View(adusa_brands_mcpt)

#Exporting Gateway CPT data
write.xlsx(adusa_brands_mcpt, "C:/Desktop/R_Monthly_adusa_brands_cpt.xlsx")
write.csv(adusa_brands_mcpt, "C:/Desktop/R_Monthly_adusa_brands_cpt.csv", row.names = FALSE)
