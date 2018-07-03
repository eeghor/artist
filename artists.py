import json
import re
from itertools import chain
from collections import OrderedDict, defaultdict, Counter
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as et
import time
import sys
import os
from pprint import pprint
import soundcloud
import spotipy
import boto3
import io
# import bson
from spotipy.oauth2 import SpotifyClientCredentials
from birdy.twitter import UserClient

class Artist:

	"""
	Collect and match artist information from multiple sources
	"""

	CRED_DIR = 'credentials'
	DATA_DIR = 'data'

	MEDIA = 'facebook twitter youtube wikipedia soundcloud equipboard instagram last.fm'.split()

	def __init__(self, create_new=False, artist_file=None):

		self.create_new = create_new
		self.ARTIST_FILE = f'{Artist.DATA_DIR}/{artist_file}'
		self.GENRE_FILE = f'{Artist.DATA_DIR}/genres.txt'

		if not self.create_new:

			self.artists = json.load(open(self.ARTIST_FILE))
			print(f'loaded {len(self.artists)} artists from {self.ARTIST_FILE}')

		else:

			print('starting from an empty artist list...')
			self.artists = []

		self.SONGKICK_API_KEY = json.load(open(f'{Artist.CRED_DIR}/songkick.json'))['songkick_api_key']
		print('loaded songkick api key...')

		self.SOUNDCLOUD_API_KEY = json.load(open(f'{Artist.CRED_DIR}/soundcloud.json'))['client_id']
		print('loaded soundcloud api key...')

		self.YOUTUBE_DEVELOPER_KEY = json.load(open(f'{Artist.CRED_DIR}/youtube.json'))['developerKey']
		print('loaded youtube developer key...')

		self.CREDENTIALS_S3 = json.load(open(f'{Artist.CRED_DIR}/s3.json'))
		
		self.DISCOGS_DUMP = f'{Artist.DATA_DIR}/discogs_20180401_artists.xml'

		try:
			self.GENRES = list({g.strip().lower() for g in open(self.GENRE_FILE).readlines() if g.strip()})
		except:
			self.GENRES = self.get_genres()
			with open(self.GENRE_FILE,'w') as f:
				for g in self.GENRES:
					f.write(f'{g}\n')

		print(f'genres: {len(self.GENRES)}')

		self.GIGERROR_ARTISTS = []

		# note that the names of gold/platinum artists get normalized straight away
		self.goldplatinum = [self.normalise_name(l.strip()) for l in open(f'{Artist.DATA_DIR}/goldplatinum-artists.txt','r').readlines() if l.strip()]
		self.billboard = [self.normalise_name(l.strip()) for l in open(f'{Artist.DATA_DIR}/billboard_artists.txt','r').readlines() if l.strip()]
		self.rollingstone = [self.normalise_name(l.strip()) for l in open(f'{Artist.DATA_DIR}/rollingstone.txt','r').readlines() if l.strip()]
		self.gigs_in_aus = [self.normalise_name(l.strip()) for l in open(f'{Artist.DATA_DIR}/data_atists_aus_gigs.txt','r').readlines() if l.strip()]
		self.award_winners = {self.normalise_name(a): reward_lst for a, reward_lst in json.load(open(f'{Artist.DATA_DIR}/award_winners.json')).items()}
		

	def get_genres(self, url='http://everynoise.com/everynoise1d.cgi?scope=all'):
		"""
		returns a list of all genres from Every Noise at Once
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
		this information is collected via searching by genre, i.e. we count on our list of genres being 
		comprehensive so having collected artists for each genres we will collect all artist

		input: genres_ is a list of genres
		"""  

		print('searching for artists by genre...')

		try:
			client_credentials_manager = SpotifyClientCredentials(**json.load(open(f'{Artist.CRED_DIR}/spotify.json')))
		except:
			print('can\'t read the spotify credentials!')
			sys.exit(0)
	
		sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
	
		MX = 4000
	
		# keep artist IDs already collected here
		artist_ids = set()
	
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
					if len(a["name"]) == len(a["name"].encode()):   # don't allow Chinese symbols
						if a['id'] not in artist_ids:
							artist_ids.add(a['id'])
							self.artists.append(a)
		  
		print(f'done. collected {len(artist_ids)} artists.' + ' elapsed time: {:.0f} min {:.0f} sec'.format(*divmod(time.time() - t0, 60)))
		  
		return self

	# def get_artists_by_name(self, name):
	# 	"""
	# 	returns a dictionary containing artist information
	# 	"""  

	# 	try:
	# 		client_credentials_manager = SpotifyClientCredentials(**json.load(open(f'{Artist.CRED_DIR}/spotify.json','r')))
	# 	except:
	# 		print(f'can\'t read spotify credentials!')
	# 		sys.exit(0)
	
	# 	sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

	# 	res = sp.search(q='artist:' + name, type='artist', limit=3)

	# 	return res

	def get_artist_from_songkick(self, name):
		"""
		get basic artist information from Songkick; it's not much, specifically:

		{'displayName': 'Placebo',   # The artist name, as it is displayed on Songkick
			'id': 324967,   # The Songkick ID of the artist
			# MusicBrainz identifiers for this artist. It is possible that an artist has multiple MusicBrainz IDs if we are not sure which is correct
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

		all we are interested at this stage is the artist name and Songkick ID
		"""
		r = requests.get(f'http://api.songkick.com/api/3.0/search/artists.json?query={name}&apikey={self.SONGKICK_API_KEY}').text 

		try:
			res = json.loads(r)["resultsPage"]["results"]["artist"][0]  # take the top search result
		except:
			return {'name': None, 'id_sk': None}

		return {'name': res['displayName'], 'id_sk': res['id']}
	
	def save(self, file_=None):
	
		if not os.path.exists(Artist.DATA_DIR):

		  os.mkdir(Artist.DATA_DIR)
	
		if self.artists:

		  json.dump(self.artists, open(self.ARTIST_FILE if not file_ else f'{Artist.DATA_DIR}/{file_}','w'))

		else:

		  print('didn\'t save artists because artist list is empty!')
	
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
		for spelled numbers starting from the compound ones like twenty two,
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
		name = re.sub(r'^(the|a)\s+','', name)

		# remove multiple white spaces
		name = wsp.sub(' ',name)
	
		# spelled numbers to numbers
		name = self.spelledout_numbers_to_numbers(name)
		
		# replace and with &
		name = name.replace(' and ',' & ')
		 
		# remove multiple white spaces
		name = wsp.sub(' ',name)
		
		# finally, strip
		name = name.strip()
		
		return name

	def normalize_all(self):
		"""
		normalize all artist names we can find in self.artists
		"""
		if not self.artists:
			print('the artist list is empty!')
			raise AssertionError

		for rc in self.artists:
			rc['name'] = self.normalise_name(rc['name'])

		return self

	def get_maxvideo_views(self):

		youtube = build('youtube', 'v3', developerKey=self.YOUTUBE_DEVELOPER_KEY)

		"""
		first we need to find a channel id if possible, so below we obtain a response like this:

		{'etag': '"DuHzAJ-eQIiCIp7p4ldoVcVAOeY/NXzz5erAJgzX9TKgE9cgokBBWBE"',
		 'items': [{'etag': '"DuHzAJ-eQIiCIp7p4ldoVcVAOeY/xPEttlr0TAylTdn8Loh1-UmL2Og"',
		            'id': {'channelId': 'UCPuKRCiD_avABa7v-m5a3kA',
		                   'kind': 'youtube#channel'},
		            'kind': 'youtube#searchResult',
		            'snippet': {'channelId': 'UCPuKRCiD_avABa7v-m5a3kA',
		                        'channelTitle': 'Kerri Chandler',
		                        'description': '',
		                        'liveBroadcastContent': 'none',
		                        'publishedAt': '2010-10-13T12:24:42.000Z',
		                        'thumbnails': {'default': {'url': 'https://yt3.ggpht.com/-15FSs4KjqzA/AAAAAAAAAAI/AAAAAAAAAAA/KkY6t0BICAw/s88-c-k-no-mo-rj-c0xffffff/photo.jpg'},
		                                       'high': {'url': 'https://yt3.ggpht.com/-15FSs4KjqzA/AAAAAAAAAAI/AAAAAAAAAAA/KkY6t0BICAw/s800-c-k-no-mo-rj-c0xffffff/photo.jpg'},
		                                       'medium': {'url': 'https://yt3.ggpht.com/-15FSs4KjqzA/AAAAAAAAAAI/AAAAAAAAAAA/KkY6t0BICAw/s240-c-k-no-mo-rj-c0xffffff/photo.jpg'}},
		                        'title': 'Kerri Chandler'}}],
		 'kind': 'youtube#searchListResponse',
		 'pageInfo': {'resultsPerPage': 5, 'totalResults': 1},
		 'regionCode': 'AU'}
		"""
		channel_id = channel_title = None

		try:
			r = yt.search().list(q="ianpooleyofc", type="channel", part="snippet").execute()
			channel_id = r['items'][0]['id']['channelId']
			channel_title = r['items'][0]['snippet']['channelTitle']
		except:
			print('can\'t find any channels')
		
		if channel_id:
			# maxResults parameter specifies the maximum number of items that should be returned in the result set
			res = yt.search().list(q="pooley", type="video", part="id,snippet", 
				maxResults=1, order="viewCount", channelId=channel_id).execute()
		
			for r in res["items"]:
				video_titles.append(r["snippet"]["title"])
				video_ids.append(r["id"]["videoId"])
			
			for i in video_ids:
				res1 = yt.videos().list(part='statistics', id=i).execute()
				for k in res1["items"]:
					print(k['statistics'])

	def drop_unpopular(self, local=True):
		"""
		following normalization, some artists in self.artists may suddenly have the same name; to disambiguate we 
		simply keep the most popular artist;

		also, drop artists whose popularity is zero
		"""
		print('dropping unpopular artists...')

		if local:
			self.artists = json.load(open(self.ARTIST_FILE))
			print(f'working with local artist file ({len(self.artists)} artists)...')

		art_before = len(self.artists)

		self.artists = [rc for rc in self.artists if rc['popularity'] > 0]

		art_after = len(self.artists)

		print(f'removed {art_after - art_before} artists...')

		names_ambig = {k: v for k, v in Counter([rc['name'] for rc in self.artists]).items() if v > 1}

		print(f'found {len(names_ambig)} ambiguous artist names...')

		name_ids_keep = {}

		for rc in self.artists:

			name_ = rc['name']

			if name_ in names_ambig:

				if name_ not in name_ids_keep:
					name_ids_keep.update({name_: {'id': rc['id'], 
													'popularity': rc['popularity']}})
				else:
					if name_ids_keep[name_]['popularity'] < rc['popularity']:
						name_ids_keep.update({name_: {'id': rc['id'], 
														'popularity': rc['popularity']}})

		dic_ = []

		for rc in self.artists:

			if rc['name'] in name_ids_keep:  # name is ambiguous

				if rc['id'] == name_ids_keep[rc['name']]['id']:
					dic_.append(rc)
			else:
				dic_.append(rc)

		self.artists = dic_

		print(f'now have {len(self.artists)} artists')

		return self

	def add_songkick_id(self):
		"""
		for every artist from self.artists try to find a Songkick id
		"""

		print('searching for songkick ids...')

		match_ = []
		nomatch_ = []

		t0 = time.time()

		for i, rc in enumerate(self.artists, 1):

			name_ = rc['name']

			# search on Songkick by name
			sk_art = self.get_artist_from_songkick(name_)

			if sk_art['name']:

				if self.normalise_name(sk_art['name']) == name_:
					rc.update({'id_sk': sk_art['id_sk']})
					match_.append(name_)
				else:
					nomatch_.append(name_)
			else:
				nomatch_.append(name_)

			if i%100 == 0:

				print(f'{i}/{len(self.artists)} ({100*i/len(self.artists):.2f}%) artists processed...')
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
		try:
			self.artists = json.load(open(f'{Artist.DATA_DIR}/artists_sk.json'))[54999:]
			print(f'working with {len(self.artists)} artists')
		except:
			print('no file found')
			sys.exit(0)

		dump_count = 11

		for i, rc in enumerate(self.artists, 1):

			n_ = rc.get('name', None)

			id_sk = rc.get('id_sk', None)

			if n_.strip() and id_sk:

				print(f'{n_}...')

				try:
					r = json.loads(requests.get(f'http://api.songkick.com/api/3.0/artists/{id_sk}/gigography.json?apikey={self.SONGKICK_API_KEY}').text)
	
					if 'resultsPage' in r:
						if 'results' in r['resultsPage']:
							if 'event' in r['resultsPage']['results']:
								rc.update({'gigs': r['resultsPage']['results']['event']})
				except:
					self.GIGERROR_ARTISTS.append({'name': n_, 'id_sk': id_sk})
					print('can\'t get this artist\'s gigs!')
					continue

			if i%300 == 0:
				print(f'{i}/{len(self.artists)} ({100*i/len(self.artists):.2f}%) artists processed...')

			if i%5000 == 0:
				
				from_ = i - 5000
				to_ = i

				dump_count += 1

				# json.dump(self.artists[from_:to_], open(f'artdump_{dump_count}.json','w'))
				self.save_to_s3(self.artists[from_:to_], f'artdump_{dump_count}.json')

				print(f'dump #{dump_count}')

		json.dump(self.GIGERROR_ARTISTS, open(f'gigs_failed_.json','w'))

		return self

	def _popularity(self, artist_name=None):
		"""
		gather popularity measures for a single artist
		"""
		if not artist_name:
			raise ValueError(f'you forgot to provide an artist\'s name!')

		inf_ = defaultdict()

		"""
		check if the artist

			- achieved a gold or platinum status according to RIAA or
			- is on one of the Billboard 'top 100' lists or
			- is known to have had gigs in Australia (according to Songkick) or
			- received some awards (in that case, add award names)
		"""
		inf_['is_goldplatinum'] = 'y' if artist_name in self.goldplatinum else 'n'
		inf_['is_billboard'] = 'y' if artist_name in self.billboard else 'n'
		inf_['is_rollingstone'] = 'y' if artist_name in self.rollingstone else 'n'
		inf_['gigs_in_aus'] = 'y' if artist_name in self.gigs_in_aus else 'n'
		inf_['awards'] = self.award_winners[artist_name] if artist_name in self.award_winners else None

		# rc.update({'is_goldplatinum': 'y' if n_ in self.goldplatinum else 'n'})
		# rc.update({'is_billboard': 'y' if n_ in self.billboard else 'n'})
		# rc.update({'is_rollingstone': 'y' if n_ in self.rollingstone else 'n'})
		# rc.update({'gigs_in_aus': 'y' if n_ in self.gigs_in_aus else 'n'})
		# rc.update({'awards': self.award_winners[n_] if n_ in self.award_winners else None})

		# show some stats

		# stats_ = defaultdict(int)

		# for r in self.artists:
		# 	for lab in 'is_goldplatinum is_billboard is_rollingstone gigs_in_aus'.split():
		# 		if r.get(lab, 'n') == 'y':
		# 			stats_[lab] += 1
		# 	if r.get('awards', None):
		# 		stats_['awards'] += 1

		# print('relatively famous artists:')

		# pprint(stats_)

		return inf_

	def get_soundcloud(self):
		"""
		collect information from Soundcloud; a sample of what's available:

		{'avatar_url': 'https://i1.sndcdn.com/avatars-000277022323-bbsjso-large.jpg',
			'city': 'Kassel',
			'comments_count': 0,
			'country': 'Germany',
			'description': 'Official Soundcloud for Milky Chance\n'
						   '\n'
						   'Management:\n'
						   'Björn Deparade (bjoern@wasted-talent.com)',
			'discogs_name': None,
			'first_name': 'Milky',
			'followers_count': 115133,
			'followings_count': 16,
			'full_name': 'Milky Chance',
			'id': 77545348,
			'kind': 'user',
			'last_modified': '2017/06/13 17:02:12 +0000',
			'last_name': 'Chance',
			'likes_count': 0,
			'myspace_name': None,
			'online': False,
			'permalink': 'milkychance',
			'permalink_url': 'http://soundcloud.com/milkychance',
			'plan': 'Pro Unlimited',
			'playlist_count': 4,
			'public_favorites_count': 0,
			'reposts_count': 1,
			'subscriptions': [{'product': {'id': 'creator-pro-unlimited',
										   'name': 'Pro Unlimited'}}],
			'track_count': 55,
			'uri': 'https://api.soundcloud.com/users/77545348',
			'username': 'Milky Chance',
			'website': 'http://www.milkychanceofficial.com',
			'website_title': 'International Website'}

		"""
		client = soundcloud.Client(client_id=self.SOUNDCLOUD_API_KEY)

		for i, rc in enumerate(self.artists, 1):

			name_ = rc['name']

			try:
				res = client.get('/users', q=name_)[0]
			except:
				print(f'couldn\'t find {name_}...')
				continue

			avail_fields = set(res.fields())
			
			for c in 'full_name username'.split():

				if (c in avail_fields) and (self.normalise_name(getattr(res, c)) == name_):

					for field_orig, field_new in zip('country city followers_count id permalink_url website'.split(),
														'country city followers_soundcloud id_sc url_sc website'.split()):
						if field_orig in avail_fields:
							_ = getattr(res, field_orig)
							if isinstance(_, int):
								rc.update({field_new: _})
							elif isinstance(_, str) and (len(_) > 1):
								rc.update({field_new: _.lower()})
					break
			else:
				print('name doesn\'t match..')

		return self

	def save_to_s3(self, what, s3file_):
		"""
		send file_ to an s3 bucket
		"""
		s3 = boto3.client('s3', **self.CREDENTIALS_S3)

		s3.upload_fileobj(Fileobj=io.BytesIO(json.dumps(what).encode()), Bucket='tega-uploads', Key=f'Igor/temp/gigographies/{s3file_}')

		
		return self

	def get_facebook_likes(self):
		"""
		how many likes an artist has at this time
		"""
		for i, rc in enumerate(self.artists, 1):

			med = rc.get('media', None)
			if med:
				fb = rc['media'].get('facebook', None)
				if fb:
					try:
						fb_soup = BeautifulSoup(requests.get(fb).text, "lxml")
						likes_ = int(fb_soup.find("span", id="PagesLikesCountDOMID").text.strip().split()[0].replace(',',''))
						rc.update({'facebook_likes': likes_})
					except:
						print(f'can\'t get likes from {fb}!')
		return self

	def get_twitter_followers(self):
		"""
		how many Twitter followers an artist has right now
		"""
		client = UserClient(**json.load(open(f'{Artist.CRED_DIR}/twitter.json')))

		for i, rc in enumerate(self.artists, 1):

			med = rc.get('media', None)
			if med:
				tw = rc['media'].get('twitter', None)
				if tw:
					try:
						tw_followers_ = client.api.users.show.get(screen_name=tw).data['followers_count']
						rc.update({'twitter_followers': tw_followers_})
					except:
						print(f'can\'t get followers from {tw}!')

		return self


	def get_discogs(self):

		def __find(element, child_name):
		
			try:
				child = element.find(child_name).text.lower().strip()
			except:
				child = None
			return child
		
		def __find_kids(element, child_name, grandchild_name):
		
			try:
				child = element.find(child_name)
			except:
				# if there's no child not much sense to proceed
				return None
		
			if not child:
				return None
		
			try:
				grandchildren = child.findall(grandchild_name)
			except:
				# what if grandchildren are missing
				return None
		
			if not grandchildren:
				return None
		
			return [v.text.lower().strip() for v in grandchildren if v.text]
		
		
		artist_lst = []
		
		c = 0
		
		# parses an XML section into an element tree incrementally, returns an iterator providing (event, elem) pairs
		# events is a list of events to report back
		for ev, a in et.iterparse(self.DISCOGS_DUMP, events=("start", "end")):
		
			if (a.tag == "artist") and (ev == "end"):
				
				# found some artist's information; make a dictionary to collect
				art_dict = defaultdict()
				
				art_dict["id_dg"] = __find(a, "id")
				art_dict["name"] = __find(a, "name")
				art_dict["real_name"] = __find(a, "realname")
				
				art_dict["name_variations"] = __find_kids(a, "namevariations", "name")
				art_dict["aliases"] = __find_kids(a, "aliases", "name")
				
				artist_urls = __find_kids(a, "urls", "url")

				art_dict["media"] = {}
				
				if artist_urls:
					
					for u in artist_urls:

						for m in Artist.MEDIA:
							if m in u:
								art_dict['media'].update({m: u})
				else:
					continue  # no media - we aren't interested; next
				
				artist_lst.append(art_dict)
		
		
		json.dump(artist_lst, open(f'{Artist.DATA_DIR}/discogs_.json', "w"), indent=4)


			
if __name__ == '__main__':
  
  art = Artist(artist_file='artists_sk.json')
  # art.normalize_all()
  # art.save('artists_n.json')
  # art.drop_unpopular()
  # art.save('artists_d.json')
  # art.add_songkick_id()
  # art.save('artists_sk.json')
  # art.add_gigs()
  # art.save('artists_gig.json')
  # art.save_to_s3(art.artists, 'artists_with_gigs.json')
  # art.get_soundcloud()
  # art.save('artists_sc.json')
  # art.get_discogs()
  # art.is_famous()
  # art.save('platinum.json')
  print(art._popularity('rolling stones'))

  print('done.')
