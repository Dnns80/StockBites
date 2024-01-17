import requests
import time
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from openai import OpenAI


class Newsletter:
    def __init__(self, customer, companies, news_articles_df):
        self.customer = customer
        self.companies = companies
        self.selected_companies_with_articles = news_articles_df[news_articles_df['company_name'].isin(self.companies)]
        self.titles = [article['title'] for article in self.selected_companies_with_articles['news_articles'].tolist()]

    def make_nice_heading(self):
        try:
            client = OpenAI(api_key=f"{os.getenv('OPENAI_API_KEY')}")

            chat_completion = client.chat.completions.create(
                messages=[
                    {"role": "system",
                     "content": "Given the list of news article titles, provide a catchy english headline for an email to catch attention of readers."},
                    {"role": "user", "content": " ".join(self.titles)},
                ],
                model="gpt-4-1106-preview",
                timeout=60,
            )
            return chat_completion.choices[0].message.content.strip('"')
        except Exception as error:
            logging.error(error)

    def send_simple_message(self):
        try:
            heading = self.make_nice_heading()
            message_data = {
                "from": "Stockbites <dailynewsletter@stockbites.pro>",
                "to": self.customer,
                "subject": heading,
                "template": "stocknews2_test",
                "v:date": f"{datetime.today().strftime('%Y-%m-%d')}"
            }

            for i, company in enumerate(self.selected_companies_with_articles['company_name'].tolist()):
                message_data[f"v:company{i + 1}"] = company
                message_data[f"v:article{i + 1}"] = True

            for i, article in enumerate(self.selected_companies_with_articles['news_articles'].tolist()):
                article_prefix = f"v:c_{i + 1}"
                message_data[f"{article_prefix}content1"] = article['text']
                message_data[f"{article_prefix}link1"] = article['url']
                message_data[f"{article_prefix}title1"] = article['title']

            return requests.post(
                "https://api.eu.mailgun.net/v3/newsletter.stockbites.pro/messages",
                auth=("api", f"{os.getenv('MAILGUN_API_KEY')}"),
                data=message_data
            )
        except Exception as error:
            logging.error(error)

    def send_newsletter(self):
        try:
            if not self.selected_companies_with_articles.empty:
                self.send_simple_message()
        except Exception as error:
            logging.error(error)
