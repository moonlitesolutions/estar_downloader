__version__ = 0.1
import requests
import logging


class EStar:
	APIS=[
	'energy-star-certified-commercial-clothes-washers',
	'energy-star-certified-residential-clothes-washers',
	'energy-star-certified-residential-dishwashers',
	'energy-star-certified-cordless-phones',
	'energy-star-certified-televisions',
	'energy-star-certified-roof-products',
	'energy-star-certified-room-air-conditioners',
	'energy-star-certified-geothermal-heat-pumps',
	'energy-star-certified-room-air-cleaners',
	'energy-star-certified-displays',
	'energy-star-certified-commercial-dishwashers',
	'energy-star-certified-water-heaters',
	'energy-star-certified-commercial-water-heaters',
	'energy-star-certified-residential-freezers',
	'energy-star-certified-residential-refrigerators',
	'energy-star-certified-computers',
	'energy-star-certified-light-commercial-hvac',
	'energy-star-certified-decorative-light-strings',
	'upcoming-energy-star-certified-furnaces',
	'upcoming-energy-star-certified-residential-clothes-dryers',
	'upcoming-energy-star-certified-light-bulbs',
	'upcoming-energy-star-certified-residential-freezers',
	'upcoming-energy-star-certified-residential-refrigerators',
	'upcoming-energy-star-certified-room-air-conditioners',
	'upcoming-energy-star-certified-telephones',
	'energy-star-certified-audio-video',
	'energy-star-certified-commercial-clothes-washers',
	'energy-star-certified-non-ahri-central-air-conditioner-equipment-and-air-source-heat-pump',
	'energy-star-certified-light-fixtures',
	'energy-star-certified-pool-pumps',
	'energy-star-certified-ceiling-fans',
	'energy-star-certified-imaging-equipment',
	'energy-star-certified-commercial-refrigerators-and-freezers',
	'energy-star-certified-small-network-equipment',
	'energy-star-certified-water-coolers',
	'energy-star-certified-boilers',
	'energy-star-certified-light-bulbs',
	'energy-star-certified-enterprise-servers',
	'energy-star-certified-commercial-ovens',
	'energy-star-certified-commercial-ice-machines',
	'energy-star-certified-commercial-fryers',
	'energy-star-certified-commercial-hot-food-holding-cabinet',
	'energy-star-certified-vending-machines',
	'energy-star-certified-dehumidifiers',
	'energy-star-certified-set-top-boxes',
	'energy-star-certified-commercial-griddles',
	'energy-star-certified-uninterruptible-power-supplies',
	'energy-star-certified-ventilating-fans',
	'energy-star-certified-commercial-steam-cookers']

	BASE_URL = 'http://data.energystar.gov/resource/'
	ADDITION = '.json'

	def __init__(self, limit=1000):
		self.rs = requests.session()
		self.limit = limit
		self.data = {}

	def _download_api(self, api_name):
		offset = 0
		url = "{}{}{}".format(self.BASE_URL, api_name, self.ADDITION)
		params = {
			'$where':'pd_id>0',
			'$offset':0,
			'$limit':self.limit
		}
		data = []
		while True:
			#Get the data, not catching any exceptions because I don't want it to live if it fails
			logging.info("GET {} {}".format(url, params))
			res = self.rs.get(url, params=params)
			if 200 <= res.status_code < 300:
				new_data = res.json()
				data.extend(new_data)
				if len(new_data) < self.limit:
					break
				else:
					params['$offset'] += self.limit
			else:					
				logging.error("Received other than 200 status code")
		for item in data:
			item['api_name'] = api_name
		return data

	def get_all(self):
		self.data = {}
		for api in self.APIS:
			self.data[api] = self._download_api(api)
		return self.data

	def write_file(self, filepathname):
		import json
		with open(filepathname, 'w') as outfile:
			json.dump([item for api in self.data.values() for item in api], outfile)
