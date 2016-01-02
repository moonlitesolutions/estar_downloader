__version__ = 0.1
import requests
import logging
import json
import datetime
from SolrClient import SolrClient

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
	'energy-star-certified-commercial-steam-cookers'
	]

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
		logging.info("Starting to fetch data for {}".format(api_name))
		while True:
			#Not catching any exceptions because I don't want it to live if it fails
			logging.debug("GET {} {}".format(url, params))
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

		logging.info("Finished fetching {}. Recieved {} items.".format(api_name, len(data)))
		self.data[api_name] = data
		return data

	def get_all(self):		
		for api in self.APIS:
			self._download_api(api)
		return self.data

	def write_file(self, filepathname):		
		with open(filepathname, 'w') as outfile:
			json.dump([item for api in self.data.values() for item in api], outfile)

	def index_to_solr(self, collection, solrclient):
		try:
			from SolrClient import SolrClient
		except ImportError:
			raise ImportError("Can't import SolrClient. Make sure it is installed")
		if type(solrclient) is not SolrClient:
			raise TypeError("solrclient argumnet should be a SolrClient instance.")
		logging.info("Preparing Data for Solr Indexing")
		out = []
		for api in self.data:
			for product in self.data[api]:
				out.append(self.prep_prod(product))

		solrclient.index_json(collection, json.dumps(out))

	def prep_prod(self, prod):
		out = {}
		for field in prod:
			try:
				try:
					dt = datetime.datetime.strptime(prod[field], "%Y-%m-%dT%H:%M:%S")
					#Adding Z here since this is for date only and solr needs it
					out[field+'_dt'] = dt.isoformat()+'Z'
					continue
				except (ValueError, TypeError):
					#Not an EStar Date field
					pass

				if prod[field].isdigit():
					out[field+'_f'] = float(prod[field])
					continue
				
				#Try to convert it to the float because isdigit returns False on floats
				try:
					out[field+'_f'] = float(prod[field])
					continue
				except ValueError:
					#Not a float
					pass

				out[field+'_s'] = prod[field]

				out['id'] = prod['pd_id']
			except:
				print(prod)
				print("ERROR WITH: {}".format(field))
		if len(out) != len(prod)+1:
			logging.error("Field count mismatch in product prep. Before: {}, After: {}".format(len(prod), len(out)))
			logging.error(prod)
			logging.error(out)
		return out


if __name__=='__main__':
	logging.basicConfig(level='DEBUG')
	e = EStar()
	e.get_all()
	s = SolrClient('http://localhost:8983/solr/')
	e.index_to_solr('estar',s)