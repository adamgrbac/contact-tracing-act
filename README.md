# NSW Health COVID-19 Contact Tracing Emailer

This project checks for new / updated locations on the ACT Health COVID-19 Contact Tracing [site](https://www.covid19.act.gov.au/act-status-and-response/act-covid-19-exposure-locations), and emails the results to a distribution list.

It was a built as a bit of a time-saver so I didn't have to keep checking the website for updates, and was also a good way to explore the [yagmail](https://pypi.org/project/yagmail/) package in python.

## Setup Required

* The email sender information and distribution list needs to be stored as a YAML file with two items:
	* sender: string - This is the email address that you intend to send emails from.
	* dist_list: list - A list of email address to send the emails to.
* On the first run, the script will also create the required sqlite database (contact_tracing_act.db) and the table within.
* __N.B__ In order use yagmail as it is used in this project, a set of API keys needs to be setup with your Google account. Once you have this, the first time you run the script, you will be prompted through a series of steps to help you create the oauth2_file.json config file.

## Scripts

* contact-tracing-act.py - The main script that does the heavy lifting.
	*  Example usage - `python contact-tracing-nsw.py`
* utils.py - Some helper functions to clean up the code, imported as a package.

## Packages Used

* pandas
* requests
* sqlite3
* urllib
* yagmail
* yaml
* bs4
* datetime
