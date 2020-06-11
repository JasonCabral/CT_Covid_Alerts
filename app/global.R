library(googlesheets4)
library(magrittr)
library(dplyr)
library(shiny)
library(ggplot2)
library(shinydashboard)
library(plotly)

gs4_auth(
  email = "jcdrummr@gmail.com",
  scopes = "https://www.googleapis.com/auth/spreadsheets",
  cache = ".secrets"
)

data <- read_sheet(ss = "1h8PgeXtEDaHQCmsFIcqDA3Hm_kVFRrR-jcEfVPfMwAI",
                   sheet = "CT_Covid_Data",
                   range = "data")

alerts <- read_sheet(ss = "1h8PgeXtEDaHQCmsFIcqDA3Hm_kVFRrR-jcEfVPfMwAI",
                     sheet = "CT_Covid_Data",
                     range = "alerts")
