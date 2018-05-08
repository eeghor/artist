import json
import re
from itertools import chain
from collections import OrderedDict, defaultdict, Counter
import requests
from bs4 import BeautifulSoup
import time
import sys
import os
from pprint import pprint
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class Artist:

	CRED_DIR = 'credentials'
	DATA_DIR = 'data'

	def __init__(self, refresh=False):

		self.refresh = refresh

		if not self.refresh:
			self.artists = json.load(open(f'{Artist.DATA_DIR}/artists_.json'))
			print(f'loaded {len(self.artists)} artists from artists_.json')
		else:
			self.artists = []

		self.SONGKICK_API_KEY = json.load(open(f'{Artist.CRED_DIR}/songkick.json'))['songkick_api_key']

	def get_genres(self, url='http://everynoise.com/everynoise1d.cgi?scope=all'):
		"""
		return a list of all genres from Every Noise at Once
		"""
		soup = BeautifulSoup(requests.get(url).text, 'lxml')
	
		genres_ = set()
	
		# find the headphone symbols (see the page)
		for _ in soup.find_all('a', class_='note'):
			genres_.add(_.parent.next_sibling.text.lower().strip())
		
		return list(genres_)
	
	def get_artists_by_genre(self, genres_):
		"""
		returns a dictionary containing artist information;
		this information is collected via searching by genre
		"""  
		try:
			client_credentials_manager = SpotifyClientCredentials(**json.load(open(f'{Artist.CRED_DIR}/spotify.json')))
		except:
			print('can\'t read the spotify credentials!')
			sys.exit(0)
	
		sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
	
		MX = 4000
	
		# keep artist names already collected here
		artist_names = set()
	
		t0 = time.time()
	
		for j, g in enumerate(genres_, 1):
			  
			print(f'collecting genre {j}/{len(genres_)} - {g}...', end='')
			  
			for _ in range(MX//50):
				  
				res = sp.search(q='genre:' + g.replace(' ',''), type='artist', limit=50, offset=_*50)
				  
				"""
				response is like this:
				  
				{'artists': {'href': 'https://api.spotify.com/v1/search?query=genre%3Apop&type=artist&offset=0&limit=50',
					   'items': [
								 {'external_urls': {'spotify': 'https://open.spotify.com/artist/6l3HvQ5sa6mXTsMTB19rO5'},
								  'followers': {'href': None, 'total': 5054392},
								  'genres': ['pop', 'pop rap', 'rap'],
								  'href': 'https://api.spotify.com/v1/artists/6l3HvQ5sa6mXTsMTB19rO5',
								  'id': '6l3HvQ5sa6mXTsMTB19rO5',  # The Spotify ID for the artist
								  'images': [{'height': 640,
											  'url': 'https://i.scdn.co/image/839defbfdeb72488b3b495e2c4e89990933f0167',
											  'width': 640},
											 {'height': 320,
											  'url': 'https://i.scdn.co/image/df0424ed9e3fd02f3c5a98dedd4307adb3df4eb3',
											  'width': 320},
											 {'height': 160,
											  'url': 'https://i.scdn.co/image/6c27976d222131de69da808b86c19c78859c1be0',
											  'width': 160}],
								  'name': 'J. Cole',
								  'popularity': 94,  # between 0 and 100, calculated from the popularity of all the artist’s tracks
								  'type': 'artist',
								  'uri': 'spotify:artist:6l3HvQ5sa6mXTsMTB19rO5'  # The **resource** identifier to locate an artist
								  },
								  ....
				  
				"""      
				for a in res['artists']['items']:
					# note that a is a dictionary with artist information
					if len(a["name"]) == len(a["name"].encode()):   # don't allow chinese symbols
						if a['name'].lower() not in artist_names:
							artist_names.add(a['name'].lower())
							self.artists.append(a)
						  
				print(len(self.artists), end="")
				print("...ok")
		  
		print("elapsed time: {:.0f} min {:.0f} sec".format(*divmod(time.time() - t0, 60)))
		  
		return self

	def get_artists_by_name(self, name):
		"""
		returns a dictionary containing artist information
		"""  

		try:
			client_credentials_manager = SpotifyClientCredentials(**json.load(open(f'{Artist.CRED_DIR}/spotify.json','r')))
		except:
			print(f'can\'t read the spotify credentials!')
			sys.exit(0)
	
		sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

		res = sp.search(q='artist:' + name, type='artist', limit=3)

		return res

	def get_artist_from_songkick(self, name):
		"""
		get basic artist information from Songkick; it's not much, specifically:
		{'displayName': 'Placebo',   # The artist name, as it is displayed on Songkick
 			'id': 324967,   # The Songkick ID of the artist
 			# MusicBrainz identifiers for this artist. It is possible that an artist has mutliple MusicBrainz IDs if we are not sure which is correct
		 	'identifier': [{'eventsHref': 'http://api.songkick.com/api/3.0/artists/mbid:81b9963b-7ff7-47f7-9afb-fe454d8db43c/calendar.json',
                 'href': 'http://api.songkick.com/api/3.0/artists/mbid:81b9963b-7ff7-47f7-9afb-fe454d8db43c.json',
                 'mbid': '81b9963b-7ff7-47f7-9afb-fe454d8db43c',
                 'setlistsHref': 'http://api.songkick.com/api/3.0/artists/mbid:81b9963b-7ff7-47f7-9afb-fe454d8db43c/setlists.json'},
                {'eventsHref': 'http://api.songkick.com/api/3.0/artists/mbid:847e8284-8582-4b0e-9c26-b042a4f49e57/calendar.json',
                 'href': 'http://api.songkick.com/api/3.0/artists/mbid:847e8284-8582-4b0e-9c26-b042a4f49e57.json',
                 'mbid': '847e8284-8582-4b0e-9c26-b042a4f49e57',
                 'setlistsHref': 'http://api.songkick.com/api/3.0/artists/mbid:847e8284-8582-4b0e-9c26-b042a4f49e57/setlists.json'}],
 		# The date until which this artist is on tour, in the form 'YYYY-MM-DD'. 'null' if this artist is not currently touring
 		'onTourUntil': '2018-06-23',
 		# The URI of the artist on Songkick
 		'uri': 'http://www.songkick.com/artists/324967-placebo?utm_source=45672&utm_medium=partner'}
		"""
		r = requests.get(f'http://api.songkick.com/api/3.0/search/artists.json?query={name}&apikey={self.SONGKICK_API_KEY}').text 

		try:
			res = json.loads(r)["resultsPage"]["results"]["artist"][0]  # take the top search result
		except:
			return {'name': None, 'id_sk': None}

		return {'name': res['displayName'], 'id_sk': res['id']}
	
	def save(self, file_):
	
		if not os.path.exists(Artist.DATA_DIR):
		  os.mkdir(Artist.DATA_DIR)
	
		if self.artists:
		  json.dump(self.artists, open(f'{Artist.DATA_DIR}/{file_}','w'))
		else:
		  print(f'didn\'t save artists to {file_} because artist list is empty!')
	
	
	def spelledout_numbers_to_numbers(self, s):
		"""
		returns string s where all spelled out numbers between 0 and 99 are
		converted to numbers
		"""
		numbers_1to9 = 'one two three four five six seven eight nine'.split() 
		mappings_1to9 = {t[0]: str(t[1]) 
							   for t in zip(numbers_1to9, range(1,10))}
		
		mappings_10to19 = {t[0]: str(t[1]) 
							   for t in zip("""ten eleven twelve thirteen fourteen fifteen 
											  sixteen seventeen eighteen nineteen""".split(), range(10,20))}
		
		numbers_20to90 = 'twenty thirty fourty fifty sixty seventy eighty ninety'.split()
		mappings_20to90 = {t[0]: str(t[1]) 
							   for t in zip(numbers_20to90, range(20,100,10))}
		
		# produce numbers like twenty one, fifty seven, etc.
		numbers_21to99 = [' '.join([s,p]) for s in numbers_20to90 for p in numbers_1to9]
		
		"""
		create an ordered dictionary mapping spelled numbers to numbers in
		digits; note that the order is important because we want to search
		for spelled numbers starting from the compount ones like twenty two,
		then try to find the rest
		"""
		
		od = OrderedDict({t[0]:t[1] 
							for t in zip(numbers_21to99, 
										 # create a list [21,22,..,29,31,..,39,41,..,99]
										 [_ for _ in chain.from_iterable([[str(_) for _ in range(int(d)*10 + 1,int(d+1)*10)] 
											   for d in range(2,10)])])})
		od.update(mappings_20to90)
		od.update(mappings_10to19)
		od.update(mappings_1to9)
		
		for w_ in od:
			  s = re.sub(r'\b' + w_ + r'\b', od[w_], s)
		
		return s
	
	def normalise_name(self, name):
		"""
		return a normalized artist name
		"""
	
		name = name.lower()
		
		# \s matches any whitespace character; 
		# this is equivalent to the class [ \t\n\r\f\v]
		wsp = re.compile(r'\s{2,}')
	
		emoji = ':) :('.split()
		
		# if its only ? it may be an artist
		if name.strip() in {'?','...'}:
			return '@artist'
		
		# label emojis
		name = re.sub(r'\s*:[\(\)]\s*',' @artist ', name)
		
		# replace separators and quotes with white spaces
		name = re.sub(r'[_\-:;/.,\"\`\']', ' ', name) 
		
		# fix ! - remove if at the end of a word, otherwise replace with i
		name = re.sub(r'\!+(?=[^\b\w])','', name)
		name = re.sub(r'\!+$','', name)
		
		name = name.replace('!','i')
		
		# remove all brackets and hyphens
		name = re.sub(r'[\[\]\{\}\(\)]','', name) 
		
		# remove the and a
		name = re.sub(r'(the|a)\s+','', name)

		# remove multiple whitespaces
		name = wsp.sub(' ',name)
	
		# spelled numbers to numbers
		name = self.spelledout_numbers_to_numbers(name)
		
		# replace and with &
		name = name.replace(' and ',' & ')
		 
		# remove multiple whitespaces
		name = wsp.sub(' ',name)
		
		# finally, strip
		name = name.strip()
		
		return name

	def normalize_all(self):
		
		if not self.artists:
			print('the artist list is empty!')
			raise AssertionError

		for artist_rec in self.artists:
			artist_rec['name'] = self.normalise_name(artist_rec['name'])

		return self

	def drop_unpopular(self):
		"""
		following normalization, some artists may suddenly have the same name; to disambiguate to 
		simply keep the most popular artist only;

		also, drop artists whose popularity is zero

		"""

		art_before = len(self.artists)

		self.artists = [artist_rec for artist_rec in self.artists if artist_rec['popularity'] > 0]

		art_after = len(self.artists)

		print(f'removed {art_after - art_before} unpopular artists...')


		names_ambig = {k: v for k, v in Counter([artist_rec['name'] for artist_rec in self.artists]).items() if v > 1}

		print(f'found {len(names_ambig)} ambiguous artist names...')

		name_ids_keep = {}

		for artist_rec in self.artists:

			name_ = artist_rec['name']

			if name_ in names_ambig:
				if name_ not in name_ids_keep:
					name_ids_keep.update({name_: {'id': artist_rec['id'], 
													'popularity': artist_rec['popularity']}})
				else:
					if name_ids_keep[name_]['popularity'] < artist_rec['popularity']:
						name_ids_keep.update({name_: {'id': artist_rec['id'], 
														'popularity': artist_rec['popularity']}})

		dic_ = []

		for artist_rec in self.artists:

			if artist_rec['name'] in name_ids_keep:  # name is ambiguous
				if artist_rec['id'] == name_ids_keep[artist_rec['name']]['id']:
					dic_.append(artist_rec)
			else:
				dic_.append(artist_rec)

		self.artists = dic_

		print(f'now have {len(self.artists)} artists')

		return self

	def add_songkick_id(self):
		"""
		for every artist on the list try to find a Songkick id
		"""
		match_ = []
		nomatch_ = []

		t0 = time.time()

		for i, artist_rec in enumerate(self.artists, 1):

			name_ = artist_rec['name']
			sk_art = self.get_artist_from_songkick(name_)
			if sk_art['name']:
				if self.normalise_name(sk_art['name']) == name_:
					artist_rec.update({'id_sk': sk_art['id_sk']})
					match_.append(name_)
				else:
					nomatch_.append(name_)
			else:
				nomatch_.append(name_)

			if i%100 == 0:

				print(f'looking for songkick ids: {i}/{len(self.artists)} ({100*i/len(self.artists):.2f}%) artists processed...')
				print(f'matched {len(match_)}, didn\'t match {len(nomatch_)}')
				print(f'time: {time.time() - t0:.0f} sec / 100')

				t0 = time.time()

		return self

	def add_gigs(self):
		"""
		add gigography from Songkick; the response look like this:

		{
 			"resultsPage": {
   			"status": "ok",
   			"results": { "event": [detailed gig descriptions here, dictionaries in this list]
   						 },
   			"perPage": 50,
    		"page": 1,
    		"totalEntries": 1179
  							}
		}

		where gig descriptions are as below

		"event": [
        {
          "type": "Concert",
          "popularity": 0.189824,
          "status": "ok",
          "displayName": "Placebo at The Rock Garden (January 23, 1995)",
          "start": {
            "time": null,
            "date": "1995-01-23",
            "datetime": null
          },
          "location": {
            "city": "London, UK",
            "lat": 51.512061,
            "lng": -0.1229647
          },
          "uri": "http://www.songkick.com/concerts/937131-placebo-at-rock-garden?utm_source=45672&utm_medium=partner",
          "id": 937131,
          "performance": [
            {
              "billingIndex": 1,
              "billing": "headline",
              "displayName": "Placebo",
              "id": 1347942,
              "artist": {
                "displayName": "Placebo",
                "identifier": [
                  {
                    "mbid": "81b9963b-7ff7-47f7-9afb-fe454d8db43c",
                    "href": "http://api.songkick.com/api/3.0/artists/mbid:81b9963b-7ff7-47f7-9afb-fe454d8db43c.json"
                  },
                  {
                    "mbid": "847e8284-8582-4b0e-9c26-b042a4f49e57",
                    "href": "http://api.songkick.com/api/3.0/artists/mbid:847e8284-8582-4b0e-9c26-b042a4f49e57.json"
                  }
                ],
                "uri": "http://www.songkick.com/artists/324967-placebo?utm_source=45672&utm_medium=partner",
                "id": 324967
              }
            }
          ],
          "venue": {
            "metroArea": {
              "displayName": "London",
              "country": {
                "displayName": "UK"
              },
              "uri": "http://www.songkick.com/metro_areas/24426-uk-london?utm_source=45672&utm_medium=partner",
              "id": 24426
            },
            "displayName": "The Rock Garden",
            "lat": 51.512061,
            "lng": -0.1229647,
            "uri": "http://www.songkick.com/venues/35994-rock-garden?utm_source=45672&utm_medium=partner",
            "id": 35994
          },
          "ageRestriction": null
        }, 
		"""

		for i, artist_rec in enumerate(self.artists, 1):

			id_sk = artist_rec.get('id_sk', None)

			if id_sk:

				r = json.loads(requests.get(f'http://api.songkick.com/api/3.0/artists/{id_sk}/gigography.json?apikey={self.SONGKICK_API_KEY}').text)

				if 'resultsPage' in r:
					if 'results' in r['resultsPage']:
						if 'event' in r['resultsPage']['results']:
							artist_rec.update({'gigs': r['resultsPage']['results']['event']})

			if i%100 == 0:
				print(f'looking for gigs: {i}/{len(self.artists)} ({100*i/len(self.artists):.2f}%) artists processed...')

			if i%1000 == 0:
				json.dump(self.artists[(i-1000):i], open(f'artdump_{i}.json','w'))

		return self

			
if __name__ == '__main__':
  
  art = Artist().normalize_all()
  art.save('artists_n.json')
  art.drop_unpopular()
  art.save('artists_d.json')
  art.add_songkick_id()
  art.save('artists_sk.json')
  art.add_gigs()
  art.save('placebo_test.json')

