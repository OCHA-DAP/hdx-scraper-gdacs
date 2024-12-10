#!/usr/bin/python
"""gdacs scraper"""

import logging
from typing import Optional

from feedparser import parse
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class GDACS:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        self._configuration = configuration
        self._retriever = retriever
        self.data = []
        self.dates = []
        self.countries = set()

    def get_data(self) -> None:
        text = self._retriever.download_text(self._configuration["base_url"])
        entries = parse(text).entries
        for entry in entries:
            iso3 = entry.gdacs_iso3
            if iso3 and iso3 != "":
                self.countries.add(iso3)
            from_date = entry.gdacs_fromdate
            to_date = entry.gdacs_todate
            self.dates.append(parse_date(from_date))
            self.dates.append(parse_date(to_date))
            event_type = entry.gdacs_eventtype
            event_type = self._configuration["disaster_conversion"].get(
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
        return

    def generate_dataset(self) -> Optional[Dataset]:
        dataset_name = self._configuration["dataset_name"]
        dataset_title = self._configuration["dataset_title"]
        dataset_time_start = min(self.dates)
        dataset_time_end = max(self.dates)
        dataset_tags = self._configuration["tags"]
        dataset_country_iso3s = self.countries

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.set_time_period(dataset_time_start, dataset_time_end)
        dataset.add_tags(dataset_tags)
        dataset.add_country_locations(dataset_country_iso3s)

        dataset.generate_resource_from_iterable(
            headers=list(self.data[0].keys()),
            iterable=self.data,
            hxltags=self._configuration["hxl_tags"],
            folder=self._retriever.temp_dir,
            filename="gdacs_rss_information.csv",
            resourcedata={
                "name": "gdacs_rss_information.csv",
                "description": " ",
            },
            encoding="utf-8-sig",
        )

        return dataset
