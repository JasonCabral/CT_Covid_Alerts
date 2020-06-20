# CT_Covid_Alerts
CT Covid Data Visualization and Alerting system


## Getting Started

### Setup Google API

#### Python

[Sheets Quickstart API](https://developers.google.com/sheets/api/quickstart/python)

[Gmail Quickstart API](https://developers.google.com/gmail/api/quickstart/go)

Step 1 will get you `credentials.json`.

Place credentials.json in working directory and run `build_serv()`. This will generate the token file for Google Authentication.

#### R

Load the googlesheets4 package.

`library(googlesheets4)`

Specify a project level directory `.secrets` which will contain the Google token. Set the `gargle_oauth_cache option` to refer to this `.secrets` directory.

`options(gargle_oauth_cache = ".secrets")`

Perform interactive auth once

`googlesheets4::sheets_auth()`

Double check that the token was cached in .secrets

`list.files(".secrets/")`

Deauthorize the session in order to try authenticating without interactivity

`sheets_deauth()`

Specify the tocken cache location, the user email and the scope in the auth call

`gs4_auth(
  email = "<your_email@gmail.com",
  scopes = "https://www.googleapis.com/auth/spreadsheets",
  cache = ".secrets"
)`

Now that auth works with the token, add the cache option to the `.Rprofile`. Include both the `.secrets` directory an `.Rprofile` file when publishing.

[credit](https://josiahparry.com/post/2020-01-13-gs4-auth/)


## Helpful Links
[Google Sheets API Documentation](https://developers.google.com/sheets/api/reference/rest)


# TO-DO

### Script
Setup venv
Convert script and automate it 


### App Main
* get data
* make viz
* Metric toggles
* Add References to CT Data and COVID Analytics
* Add github Link, email or issues link

### Alerts Panel
* get alerts
* allow add and remove alerts (some sort of protection would be great)
* write to Sheets on save button (or delete button)

