import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime

from ..config import OUTPUTS_DIR


class pe_Client:
    """
    Object to make sure we get to the web "States" needed to parse the webpage without errors.
    Could not get the XMLHTTP Request from the webpage directly - That would be more straightforward.
    """

    def __init__(self, root_url, login_page_url, login_action_url):
        self.root_url = root_url
        self.login_page_url = login_page_url
        self.login_action_url = login_action_url
        self.session = requests.Session()
        self.logged_in_state()

    def logged_in_state(self):
        """
        Take self.Session to a logged in state preserving the ASP.NET needed payload: VIEWSTATE and EVENTVALIDATION.
        """
        #### First HTTP request (GET)
        # Fetch the login page
        self.response = self.session.get(self.login_page_url)
        self.soup_1 = BeautifulSoup(self.response.text, "html.parser")
        self.viewstate = self.soup_1.find("input", attrs={"name": "__VIEWSTATE"})[
            "value"
        ]
        self.eventvalidation = self.soup_1.find(
            "input", attrs={"name": "__EVENTVALIDATION"}
        )["value"]
        payload = {
            "__VIEWSTATE": self.viewstate,
            "__EVENTVALIDATION": self.eventvalidation,
            "txt_username": "username",
            "txt_password": "password",
            "Button1": "Login",
        }
        #### Second HTTP request (POST)
        self.response = self.session.post(self.login_page_url, data=payload)
        self.soup_2 = BeautifulSoup(self.response.text, "html.parser")
        self.viewstate = self.soup_2.find("input", attrs={"name": "__VIEWSTATE"})[
            "value"
        ]
        self.eventvalidation = self.soup_2.find(
            "input", attrs={"name": "__EVENTVALIDATION"}
        )["value"]
        data = {
            "__VIEWSTATE": self.viewstate,
            "__EVENTVALIDATION": self.eventvalidation,
        }
        #### Third HTTP request (GET) Default Page
        self.response = self.session.get(self.login_action_url, data=payload)
        self.soup_3 = BeautifulSoup(self.response.text, "html.parser")
        self.viewstate = self.soup_3.find("input", attrs={"name": "__VIEWSTATE"})[
            "value"
        ]
        self.eventvalidation = self.soup_3.find(
            "input", attrs={"name": "__EVENTVALIDATION"}
        )["value"]

        self.iframes = [
            item["src"]
            for item in self.soup_3.find_all("iframe")
            if item["src"]
            in [
                "Demanda.aspx?page=true",
                "Generacion.aspx?page=true",
                "Graficas.aspx?page=true",
            ]
        ]
        self.page_date = [
            item.text for item in self.soup_3.find_all("span") if "date" in item["id"]
        ][0]
        if self.response.ok == True:
            print("Logged in succesfully.")

    def get_date_state(self, date):
        """
        Take self.Session to a logged in state for a specific date.
        """
        self.date = date
        data = {
            "ctl00_ContentPlaceHolder1_TabContainer1_ClientState": '{"ActiveTabIndex":0,"TabState":[true,true,true,true,true,true,true]}',
            "__VIEWSTATE": self.viewstate,
            "__EVENTVALIDATION": self.eventvalidation,
            "ctl00$txt_date": self.date,
            "ctl00$IMGB_GO.x": "09",
            "ctl00$IMGB_GO.y": "20",
        }

        self.response = self.session.post(self.login_action_url, data=data)
        self.soup = BeautifulSoup(self.response.text, "html.parser")
        ##### Let's see if we need to go into this State or not... maybe we just need to repeat the __viewstate and __eventstate from the previous state.
        # self.viewstate = self.soup.find('input', attrs={'name': '__VIEWSTATE'})['value']
        # self.eventvalidation = self.soup.find('input', attrs={'name': '__EVENTVALIDATION'})['value']
        if self.response.ok == True:
            print(f"Page moved to {self.date} succesfully.")
        else:
            print("Error.")

    def get_all_raw_reports(self):
        self.get_Resumen_report()
        print(f"{self.date} Resumen Report loaded")
        self.get_report()
        print(f"{self.date} Generacion Report loaded")
        self.get_Demanda_report()
        print(f"{self.date} Demanda Report loaded")

    def get_Resumen_report(self):
        """
        Downloading Resumen Report Tables in Raw format in ./outputs folder
        """
        if not os.path.exists(OUTPUTS_DIR):
            os.makedirs(OUTPUTS_DIR)
        # If we are going to parse them all we do not need the 'This Year', 'Last Week' columns, only TODAY.
        # Delete row "Station Temperature"
        # Delete row with SOUTHERN thingies.
        # Melt it all into a single numeric column
        self.Resumen_response = self.session.get(self.root_url + self.iframes[0])
        self.Resumen_soup = BeautifulSoup(self.Resumen_response.text, "html.parser")
        if self.Resumen_soup.find("head").text.strip() == "DataIsNotValidated":
            print("Data is not available yet.")
        else:
            df_list = pd.read_html(self.Resumen_soup.find("table").prettify())
            self.Resumen_list = []
            ### Main Resumen table
            df = df_list[3]
            df["date"] = self.date
            df.to_csv(
                os.path.join(
                    OUTPUTS_DIR, f"Resumen_report1_{self.date.replace('/','')}.csv"
                )
            )
            self.Resumen_list.append(df)
            ### Temp & Humid Green
            try:
                df = df_list[4]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report2_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            ### Temp & Humid Blue
            try:
                df = df_list[5]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report3_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            ### Cuzco Green
            # If we are going to parse them all we do not need the 'This Year', 'Last Week' columns, only TODAY.
            # Use the same columns from the main table.
            try:
                df = df_list[6]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report4_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            ### Cuzco Green
            try:
                df = df_list[7]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report5_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            ### Cuzco Blue
            # Do not need the 'This Year', 'Last Week' columns, only TODAY.
            # Use the same columns from the main table.
            try:
                df = df_list[8]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report6_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            ### Cuzco Blue
            try:
                df = df_list[9]
                df["date"] = self.date
                df.to_csv(
                    os.path.join(
                        OUTPUTS_DIR, f"Resumen_report7_{self.date.replace('/','')}.csv"
                    )
                )
                self.Resumen_list.append(df)
            except:
                pass
            print("Tables saved.")

    def get_report(self):
        """
        Downloading Generacion Report Tables in Raw format in ./outputs folder.
        """
        if not os.path.exists(OUTPUTS_DIR):
            os.makedirs(OUTPUTS_DIR)
        self.Generacion_response = self.session.get(self.root_url + self.iframes[1])
        self.Generacion_soup = BeautifulSoup(
            self.Generacion_response.text, "html.parser"
        )
        df_list = pd.read_html(self.Generacion_soup.find("table").prettify())
        self.Generacion_list = []
        try:
            df = df_list[4]
            df["date"] = self.date
            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"report1_{self.date.replace('/','')}.csv")
            )
            self.Generacion_list.append(df)
        except:
            pass
        try:
            df = df_list[5]
            df["date"] = self.date
            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"report2_{self.date.replace('/','')}.csv")
            )
            self.Generacion_list.append(df)
        except:
            pass
        print("Tables saved.")

    def get_Demanda_report(self):
        """
        Downloading Load Report Tables in Raw format in ./outputs folder.
        """
        if not os.path.exists(OUTPUTS_DIR):
            os.makedirs(OUTPUTS_DIR)
        self.load_response = self.session.get(self.root_url + self.iframes[2])
        self.load_soup = BeautifulSoup(self.load_response.text, "html.parser")
        df_list = pd.read_html(self.load_soup.find("table").prettify())
        self.load_list = []
        try:
            df = df_list[4]
            df["date"] = self.date
            df.to_csv(
                os.path.join(OUTPUTS_DIR, f"report21_{self.date.replace('/','')}.csv")
            )
            self.load_list.append(df)
        except:
            pass
        print("Tables saved.")
