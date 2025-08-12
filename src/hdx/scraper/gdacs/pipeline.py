#!/usr/bin/python
"""gdacs scraper"""

import logging
from datetime import datetime
from typing import Optional

from feedparser import parse
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self.configuration = configuration
        self.retriever = retriever
        self.data = []
        self.dates = []
        self.countries = set()

    def parse_feed(self, previous_build_date) -> (datetime, bool):
        rssfile = self.retriever.download_file(self.configuration["base_url"])
        feed = parse(rssfile)
        last_build_date = parse_date(feed.feed.updated)
        if last_build_date <= previous_build_date:
            return previous_build_date, False
        for entry in feed.entries:
            iso3 = entry.gdacs_iso3
            if iso3 and iso3 != "":
                self.countries.add(iso3)
            from_date = entry.gdacs_fromdate
            to_date = entry.gdacs_todate
            self.dates.append(parse_date(from_date))
            self.dates.append(parse_date(to_date))
            event_type = entry.gdacs_eventtype
            event_type = self.configuration["disaster_conversion"].get(
                event_type, event_type
            )
            self.data.append(
                {
                    "id": entry.id,
                    "iso3": iso3,
                    "country": entry.gdacs_country,
                    "title": entry.title,
                    "summary": entry.summary,
                    "event_type": event_type,
                    "severity_unit": entry.gdacs_severity["unit"],
                    "severity_value": entry.gdacs_severity["value"],
                    "source": entry.gdacs_description,
                    "from_date": from_date,
                    "to_date": to_date,
                    "link": entry.link,
                    "geo_lat": entry.geo_lat,
                    "geo_long": entry.geo_long,
                    "gdacs_bbox": entry.gdacs_bbox,
                }
            )
        return last_build_date, True

    def generate_dataset(self) -> Optional[Dataset]:
        dataset_name = self.configuration["dataset_name"]
        dataset_title = self.configuration["dataset_title"]
        dataset_time_start = min(self.dates)
        dataset_time_end = max(self.dates)
        dataset_tags = self.configuration["tags"]
        dataset_country_iso3s = self.countries

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_start, dataset_time_end)
        dataset.add_tags(dataset_tags)
        for country in dataset_country_iso3s:
            try:
                dataset.add_country_location(country)
            except HDXError:
                logger.error(f"Could not add country location for {country}")

        dataset.generate_resource_from_iterable(
            headers=list(self.data[0].keys()),
            iterable=self.data,
            hxltags=self.configuration["hxl_tags"],
            folder=self.retriever.temp_dir,
            filename="gdacs_rss_information.csv",
            resourcedata={
                "name": "gdacs_rss_information.csv",
                "description": " ",
            },
            encoding="utf-8-sig",
        )

        return dataset
