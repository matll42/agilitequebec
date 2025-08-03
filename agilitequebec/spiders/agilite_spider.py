from pathlib import Path

from geopy.geocoders import Nominatim
import json
import re
import redis
import scrapy
from prettytable import PrettyTable


class AgiliteSpider(scrapy.Spider):
    name = "agilite"

    async def start(self):
        urls = [
            "https://www.agilitequebec.com/html/activitesQC.html",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def unspace(self, text):
        """Remove extra spaces from text."""
        return re.sub('\s+', ' ', text.strip())

    def parse(self, response):
        date = response.xpath("//em[contains(text(),'Mis à jour')]/text()").get()
        cleanDate = self.unspace(re.match('.*\D(\d+\s+\w+\s+\d+)', date, flags=0).group(1))
        self.log(cleanDate)

        redisAgilite = redis.Redis(host='localhost', port=16379, db=0)

        events = response.xpath("//body/table[2]/tr/td[not(@colspan=5)]/..")

        table = PrettyTable()
        table.field_names = ["Place", "City", "Lat", "Lng", "Date", "Judges", "Runs", "Info"]
        
        cleanEvents = [];


        for event in events:
            place = event.xpath('./td[1]/font[1]/text()').getall()
            city = re.sub('^\s+', '', place[len(place)-1])
            if (not(redisAgilite.exists(city))):
                self.log(f"Geocoding {city}...")
                geolocator = redisAgilite(user_agent="agilitequebec")
                location = geolocator.geocode(city)
                redisAgilite.set(city, json.dumps({ "latitude": location.latitude, "longitude": location.longitude }))
            else:
                self.log(f"Using cached location for {city}...")
                location_data = json.loads(redisAgilite.get(city))
                location = type('Location', (object,), location_data)()
                self.log(f"Cached location for {city}: {location}")
            
            if location:
                latitude = location.latitude
                longitude = location.longitude
                self.log(f"Geocoded {city}: {latitude}, {longitude}")
            else:
                self.log(f"Could not geocode {city}")

            if not place:
                place = ["N/A"] # Fallback if no place is found 
            else:
                place = [self.unspace("".join(place[:-1]))]
            dates = event.xpath('./td[2]/font[1]/text()').getall()
            judges = event.xpath('./td[3]/font[1]/text()').getall()
            if not judges:
                judges = ["N/A"]
            else:
                judges = [self.unspace(judge) for judge in judges]
            runs = event.xpath('./td[4]/font[1]/text()').getall()
            if not runs:
                runs = ["N/A"]
            else:
                runs = [self.unspace(run) for run in runs]
            info = event.xpath('./td[5]/font[1]/text()').getall()
            if not info:
                info = ["N/A"]
            else:
                info = [self.unspace(i) for i in info]
            event_date = dates[0] if dates else None

            table.add_row([place, city, latitude, longitude, event_date, judges, runs, info])


            self.log(event)

            cleanEvents.append({
                "place": place,
                "city": city,
                "latitude": latitude,
                "longitude": longitude,
                "date": event_date,
                "judges": judges,
                "runs": runs,
                "info": info
            })
        
        print(table)

        redisAgilite.set("events", json.dumps(cleanEvents))
        redisAgilite.set("last_update", cleanDate)

        with open("events.json", "w") as file:
            json.dump({'last_generation': datetime.now().isoformat(), 'last_update': cleanDate, 'events': cleanEvents}, file)
            

        filename = f"agilitequebec.html"
        Path(filename).write_bytes(response.body)
        self.log(f"Saved file {filename}")