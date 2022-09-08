import json
import datetime
import csv

from scrapy import Request, Spider, signals


class DraftKingsSpider(Spider):
    name = 'draftkings'

    start_urls = [
        # 'https://sportsbook.draftkings.com/leagues/baseball/mlb',
        'https://sportsbook.draftkings.com/leagues/football/nfl'
    ]
    custom_settings = {
        # "COOKIES_ENABLED": True,
        # 'CRAWLERA_ENABLED': True,
        'FEED_EXPORT_FIELDS': [
            'Team',
            'date_time',
            'Props',
            'category',
            'Player',
            'Over',
            'Over 1',
            'Over 2',
            'Under',
            'Under 1',
            'Under 2'
        ],
        'FEED_EXPORT_ENCODING': 'UTF-8',
        'FEEDS': {
            f"{name}.csv": {"format": "csv"}
        },

        'USER_AGENT': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/98.0.4758.102 Safari/537.36",
        'CRAWLERA_APIKEY': '15cf7b06d9eb49cd971fadeba28db14a',
        'CONCURRENT_REQUESTS': 32,
        # "DOWNLOAD_DELAY": 3,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,

        },

    }
    start_date = None
    end_date = None
    td = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(DraftKingsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        # if self.td:
        #     headers = self.td[0].keys()
        #     with open(f'{spider.name}_TD.csv', 'w', newline='') as output_file:
        #         dict_writer = csv.DictWriter(output_file, headers)
        #         dict_writer.writeheader()
        #         dict_writer.writerows(self.td)

        print('Closing {} spider'.format(spider.name))

    def parse(self, response):

        # with open('date.txt') as f:
        #     lines = f.readlines()
        # if len(lines) == 2:
        #     self.start_date = datetime.datetime.strptime(lines[0].strip(), '%m-%d-%Y').date()
        #     self.end_date = datetime.datetime.strptime(lines[1].strip(), '%m-%d-%Y').date()
        self.start_date = datetime.datetime.strptime("9-9-2022", '%m-%d-%Y').date()
        self.end_date = datetime.datetime.strptime('9-12-2022', '%m-%d-%Y').date()

        teams_links = response.css(
            ".parlay-card-10-a .sportsbook-table .sportsbook-table__body .event-cell-link::attr(href)").extract()
        teams_links = list(dict.fromkeys(teams_links))

        for team in teams_links:
            url = f'https://sportsbook-us-ny.draftkings.com//sites/US-NY-SB/api/v3/event/{team.split("/")[-1]}?format=json'
            yield Request(url=url, callback=self.parse_api)

    def parse_api(self, response):

        try:
            data = json.loads(response.text)

            events = data['eventCategories']
            team_name = f"{data['event']['teamName1']} VS {data['event']['teamName2']}"
            date = data['event']['startDate']

            try:
                date = date.split(".0000")[0].replace("T", " ")
            except:
                date = data['event']['startDate']

            date_value = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date()
            if self.start_date and self.end_date:
                if date_value >= self.start_date and date_value <= self.end_date:

                    Batter = []
                    pitcher = []
                    for event in events:
                        name = event['name']
                        if name == 'Batter Props':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    obj = {}
                                    obj['Team'] = team_name
                                    obj["date_time"] = date
                                    obj['Props'] = name
                                    obj['category'] = category
                                    obj['Player'] = offer['outcomes'][0]['participant']
                                    obj[offer['outcomes'][0]['label']] = f"O"
                                    obj[f"{offer['outcomes'][0]['label']} 1"] = offer['outcomes'][0]['line']
                                    obj[f"{offer['outcomes'][0]['label']} 2"] = offer['outcomes'][0]['oddsAmerican']

                                    obj[offer['outcomes'][1]['label']] = f"U"
                                    obj[f"{offer['outcomes'][1]['label']} 1"] = offer['outcomes'][1]['line']
                                    obj[f"{offer['outcomes'][1]['label']} 2"] = offer['outcomes'][1]['oddsAmerican']

                                    yield obj

                        if name == 'Pitcher Props':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    obj = {}
                                    obj['Team'] = team_name
                                    obj["date_time"] = date
                                    obj['Props'] = name
                                    obj['category'] = category
                                    obj['Player'] = offer['outcomes'][0]['participant']
                                    if category == 'To Record a Win':
                                        obj['Over'] = f""
                                        obj[f"Over 1"] = offer['outcomes'][0]['label']
                                        obj[f"Over 2"] = offer['outcomes'][0]['oddsAmerican']

                                        obj['Under'] = f""
                                        obj[f"Under 1"] = offer['outcomes'][1]['label']
                                        obj[f"Under 2"] = offer['outcomes'][1]['oddsAmerican']

                                    else:
                                        obj[offer['outcomes'][0]['label']] = f"O"
                                        obj[f"{offer['outcomes'][0]['label']} 1"] = offer['outcomes'][0]['line']
                                        obj[f"{offer['outcomes'][0]['label']} 2"] = offer['outcomes'][0]['oddsAmerican']

                                        obj[offer['outcomes'][1]['label']] = f"U"
                                        obj[f"{offer['outcomes'][1]['label']} 1"] = offer['outcomes'][1]['line']
                                        obj[f"{offer['outcomes'][1]['label']} 2"] = offer['outcomes'][1]['oddsAmerican']

                                    yield obj

                        if name == 'TD Scorers':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    outcomes = offer['outcomes']
                                    for outcome in outcomes:
                                        if category == 'TD Scorer':
                                            obj = {}
                                            obj['Team'] = team_name
                                            obj["date_time"] = date
                                            obj['Props'] = name
                                            obj['category'] = category
                                            obj['Player'] = outcome['label']
                                            obj[f'Under 2'] = f"{outcome['oddsAmerican']}"
                                            if outcome["criterionName"] == 'Anytime Scorer':
                                                # self.td.append(obj)
                                                yield obj

                                        # if category != 'TD Scorer':
                                        #
                                        #     obj = {}
                                        #     obj['Team'] = team_name
                                        #     obj["date_time"] = date
                                        #     obj['Props'] = name
                                        #     obj['category'] = category
                                        #     obj['Player'] = outcome['label']
                                        #     obj[f'Under 2'] = f"{outcome['oddsAmerican']}"
                                        #     # self.td.append(obj)
                                        #     yield obj

                        if name == 'Passing Props':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    obj = {}
                                    obj['Team'] = team_name
                                    obj["date_time"] = date
                                    obj['Props'] = name
                                    obj['category'] = category
                                    obj['Player'] = offer['outcomes'][0]['participant']
                                    obj[offer['outcomes'][0]['label']] = f"O"
                                    obj[f"{offer['outcomes'][0]['label']} 1"] = offer['outcomes'][0]['line']
                                    obj[f"{offer['outcomes'][0]['label']} 2"] = offer['outcomes'][0]['oddsAmerican']

                                    obj[offer['outcomes'][1]['label']] = f"U"
                                    obj[f"{offer['outcomes'][1]['label']} 1"] = offer['outcomes'][1]['line']
                                    obj[f"{offer['outcomes'][1]['label']} 2"] = offer['outcomes'][1]['oddsAmerican']

                                    yield obj

                        if name == 'Rush/Rec Props':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    obj = {}
                                    obj['Team'] = team_name
                                    obj["date_time"] = date
                                    obj['Props'] = name
                                    obj['category'] = category
                                    obj['Player'] = offer['outcomes'][0]['participant']
                                    obj[offer['outcomes'][0]['label']] = f"O"
                                    obj[f"{offer['outcomes'][0]['label']} 1"] = offer['outcomes'][0]['line']
                                    obj[f"{offer['outcomes'][0]['label']} 2"] = offer['outcomes'][0]['oddsAmerican']

                                    obj[offer['outcomes'][1]['label']] = f"U"
                                    obj[f"{offer['outcomes'][1]['label']} 1"] = offer['outcomes'][1]['line']
                                    obj[f"{offer['outcomes'][1]['label']} 2"] = offer['outcomes'][1]['oddsAmerican']

                                    yield obj

                        if name == 'D/ST Props':
                            values = event['componentizedOffers']
                            for cat in values:
                                category = cat['subcategoryName']
                                offers = cat['offers'][0]
                                for offer in offers:
                                    obj = {}
                                    obj['Team'] = team_name
                                    obj["date_time"] = date
                                    obj['Props'] = name
                                    obj['category'] = category
                                    obj['Player'] = offer['outcomes'][0]['participant']
                                    obj[offer['outcomes'][0]['label']] = f"O"
                                    obj[f"{offer['outcomes'][0]['label']} 1"] = offer['outcomes'][0]['line']
                                    obj[f"{offer['outcomes'][0]['label']} 2"] = offer['outcomes'][0]['oddsAmerican']

                                    obj[offer['outcomes'][1]['label']] = f"U"
                                    obj[f"{offer['outcomes'][1]['label']} 1"] = offer['outcomes'][1]['line']
                                    obj[f"{offer['outcomes'][1]['label']} 2"] = offer['outcomes'][1]['oddsAmerican']
                                    yield obj

                    # print("ok")
                else:
                    self.logger.info(f"{team_name} Match not fall under given dates.")
        except Exception as e:
            yield Request(url=response.url, callback=self.parse_api)
