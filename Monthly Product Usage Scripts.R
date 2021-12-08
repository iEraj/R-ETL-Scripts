

#ETL script to wrangle with different data sources, clean, filter, and join the data for Product Usage reporting.
#Monthly Product Usage by User ID and models and each month of the year.
#Output files, CSV files, from this script will be feeding the Power BI Dashboards for fast and streamlined approach.

######################################################### Monthly Data ####################################################################

#Loading the usage data
library(readxl)
ADUSA_Monthly_Usage_Data <- read_excel("C:/Desktop/Monthly Usage Data.xlsx", sheet = "ADUSA Monthly Data")
View(ADUSA_Monthly_Usage_Data)

#High level summary of the usage data
library(dplyr)
glimpse(ADUSA_Monthly_Usage_Data)

#Adding column Model
library(dplyr)
library(readr)

Usage_Data_m <- mutate(ADUSA_Monthly_Usage_Data, Model = ifelse(Models %in% "AHOLD_RETAIL_ADVANTAGE", "Ahold POS",
                                               ifelse(Models %in% "AHOLD_FSP", "Ahold FSP",
                                                      ifelse(Models %in% "AHOLD_PNL", "Ahold PNL",
                                                             ifelse(Models %in% "AHOLD_TAP", "Ahold TAP",
                                                                    ifelse(Models %in% "AHOLD_SC", "Ahold SC",
                                                                           ifelse(Models %in% "DELHAIZE_RETAIL_ADVANTAGE", "Delhaize POS",
                                                                                  ifelse(Models %in% "DELHAIZE_FSP", "Delhaize FSP",
                                                                                         ifelse(Models %in% "DELHAIZE_PNL", "Delhaize PNL",
                                                                                                ifelse(Models %in% "DELHAIZE_TAP", "Delhaize TAP",
                                                                                                       ifelse(Models %in% "DELHAIZE_SC", "Delhaize SC","NA")))))))))))

View(Usage_Data_m)
glimpse(Usage_Data_m)

Usage_Data_mb <- mutate(Usage_Data_m, Banner = ifelse(grepl("AHOLD", Models), "Ahold",
                                      ifelse(grepl("DELHAIZE", Models), "Delhaize", "NA")))

View(Usage_Data_mb)
glimpse(Usage_Data_mb)

#Loading the User list
library(readxl)
User_Lists <- read_excel("C:/Desktop/User Lists.xlsx", sheet = "ADUSA", col_types = c("text", "text", "text", "text", "text"))
View(User_Lists)
glimpse(User_Lists)

library(dplyr)
library(readr)
library(openxlsx)
library(readxl)
library(writexl)

#Joining the data on Unify ID
joined_data_m <- inner_join(Usage_Data_mb, User_Lists, 
                           by = c("User" = "Unify ID"))
View(joined_data_m)

#Find Unknown IDs, IDs not in the User List file
Unknown_IDs_m <- anti_join(Usage_Data_mb, User_Lists, 
                           by = c("User" = "Unify ID"))

View(Unknown_IDs_m)

#Exporting the Unknown IDs
write.xlsx(Unknown_IDs_m, "C:/Desktop/R_Unknown_IDs_m.xlsx")

#Filtering the ADUSA Gateway data
Gateway_data_m <- select(filter(joined_data_m, Department == "Gateway"), -Brand)
View(Gateway_data_m)


library(openxlsx)
library(readxl)
library(writexl)
#Exporting the Gateway usage data
write.xlsx(Gateway_data_m, "C:/Desktop/R_Gateway_Monthly_Usage.xlsx")
write.csv(Gateway_data_m, "C:/Desktop/R_Gateway_Monthly_Usage.csv", row.names = FALSE)


#Filtering the ADUSA Brands data, excluding Department Gateway and un-selecting Manufacturer column
ADUSA_Brands_m <- select(filter(joined_data_m, Department != "Gateway" | is.na(Department)), -Manufacturer)
View(ADUSA_Brands_m)

ADUSA_Brands_m <- mutate(ADUSA_Brands_m, Banner = ifelse(grepl("Food Lion", Brand), "Delhaize",
                                                     ifelse(grepl("Hannaford", Brand), "Delhaize",
                                                            ifelse(grepl("RBS/PDL", Brand), "RBS/PDL", "Ahold"))))

View(ADUSA_Brands_m)

#Exporting results both in Excel and CVS formats
library(openxlsx)
library(readxl)
library(writexl)
write.xlsx(ADUSA_Brands_m, "C:/Desktop/R_ADUSA_Monthly_Usage.xlsx")
write.csv(ADUSA_Brands_m, "C:/Desktop/R_ADUSA_Monthly_Usage.csv", row.names = FALSE)

library(dplyr)
ADUSA_Brands_mb <- ADUSA_Brands_m %>%
  group_by(Brand, `Display Name`, User, Time) %>%
  summarize(`Sum_No of Report Executions` = sum(`No of Report Executions`, na.rm = FALSE))
View(ADUSA_Brands_mb)

ADUSA_Brands_Unique_Users_m <- mutate(ADUSA_Brands_mb, Banner = ifelse(grepl("Food Lion", Brand), "Delhaize",
                                                       ifelse(grepl("Hannaford", Brand), "Delhaize",
                                                              ifelse(grepl("RBS/PDL", Brand), "RBS/PDL", "Ahold"))))
View(ADUSA_Brands_Unique_Users_m)

#Export the ADUSA Unique Users both in Excel and CSV formats
library(openxlsx)
library(readxl)
library(writexl)
write.xlsx(ADUSA_Brands_Unique_Users_m, "C:/Desktop/R_Unique_Monthly_Users.xlsx")
write.csv(ADUSA_Brands_Unique_Users_m, "C:/Desktop/R_Unique_Monthly_Users.csv", row.names = FALSE)

#Monthly Active User Rates MAU, engagement rates
library(dplyr)
ADUSA_MAU_B <- ADUSA_Brands_Unique_Users_m %>%
  group_by(Banner, Brand, Time) %>%
   count()
View(ADUSA_MAU_B)
glimpse(ADUSA_MAU_B)

#Filtering the ADUSA Users list, selecting relevant columns and un-selecting irrelevant columns
ADUSA_Users_MA <- select(filter(User_Lists, Brand != ""), -`Display Name`, -Department, -Manufacturer)
View(ADUSA_Users_MA)

ADUSA_Users_MB<- mutate(ADUSA_Users_MA, Banner = ifelse(grepl("Food Lion", Brand), "Delhaize",
                                                                     ifelse(grepl("Hannaford", Brand), "Delhaize", 
                                                                            ifelse(grepl("RBS/PDL", Brand), "RBS/PDL", "Ahold"))))
View(ADUSA_Users_MB)

#Group by Banner, Brand, i.e. pivoting the data
library(dplyr)
ADUSA_Users_m <- ADUSA_Users_MB %>%
  group_by(Banner, Brand) %>%
  count()
View(ADUSA_Users_m)
glimpse(ADUSA_Users_m)

#Monthly Active User Rates, i.e. joining two tables on Brand name
ADUSA_MAUR_A <- inner_join(ADUSA_MAU_B, ADUSA_Users_m, 
                          by = c("Brand" = "Brand"))
View(ADUSA_MAUR_A)

library(dplyr)
ADUSA_MAUR_B <- select(ADUSA_MAUR_A, Banner.x, Brand, Time, n.y, n.x)
View(ADUSA_MAUR_B)

library(data.table)
setnames(ADUSA_MAUR_B, "Banner.x", "Banner")
setnames(ADUSA_MAUR_B, "n.y", "Total Unify Users")
setnames(ADUSA_MAUR_B, "n.x", "Monthly Unique Users")
View(ADUSA_MAUR_B)

#Exporting Weekly Active User Rates both in Excel and CSV formats
library(openxlsx)
library(readxl)
library(writexl)
write.xlsx(ADUSA_MAUR_B, "C:/Desktop/R_ADUSA_Monthly_Active_User_Rates.xlsx")
write.csv(ADUSA_MAUR_B, "C:/Desktop/UR_ADUSA_Monthly_Active_User_Rates.csv", row.names = FALSE)
