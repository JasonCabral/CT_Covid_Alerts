library(googlesheets4)
library(magrittr)
library(dplyr)



gs4_auth(
  email = "jcdrummr@gmail.com",
  scopes = "https://www.googleapis.com/auth/spreadsheets",
  cache = ".secrets"
)

data <- read_sheet(ss = "1h8PgeXtEDaHQCmsFIcqDA3Hm_kVFRrR-jcEfVPfMwAI",
                   sheet = "CT_Covid_Data",
                   range = "data")

# https://docs.google.com/spreadsheets/d/1h8PgeXtEDaHQCmsFIcqDA3Hm_kVFRrR-jcEfVPfMwAI/edit#gid=1625791225