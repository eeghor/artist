import json
import re
from itertools import chain
from collections import OrderedDict
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import time
import sys
import os
from pprint import pprint
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class Artist:

  CRED_DIR = 'credentials'
  DATA_DIR = 'data'

  def __init__(self):

    self.artists = []

  def get_genres(url='http://everynoise.com/everynoise1d.cgi?scope=all'):
    """
    return a list of all genres from Every Noise at Once
    """
    soup = BeautifulSoup(requests.get(url).text, 'lxml')

    genres_ = set()

    # find the headphone symbols (see the page)
    for _ in soup.find_all('a', class_='note'):
        genres_.add(_.parent.next_sibling.text.lower().strip())
    
    return list(genres_)

  def get_artists_by_genre():
      """
      returns a dictionary containing artist information;
      this information is collected via searching by genre
      """  
  
      try:
        client_credentials_manager = SpotifyClientCredentials(**json.load(open(f'{Artist.CRED_DIR}/{spotify.json}','r')))
      except:
        print(f'can\'t read the spotify credentials!')
        sys.exit(0)
  
      sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
  
      MX = 2000
  
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

  def save(self, file_):

    if not os.path.exists(Artist.DATA_DIR):
      os.mkdir(Artist.DATA_DIR)

    if self.artists:
      json.dump(self.artists, open(f'{Artist.DATA_DIR}/{file_}','w'))
    else:
      print(f'didn\'t save artists to {file_} because artist list is empty!')


  def spelledout_numbers_to_numbers(s):
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

  def normalise_name(name):

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
    
    # replace and with &
    name = name.replace(' and ',' & ')
     
    # remove multiple whitespaces
    name = wsp.sub(' ',name)
    
    # finally, strip
    name = name.strip()
    
    return name

            
if __name__ == '__main__':
  
  art = Artist()  
  print(art.normalise_name('thirteen and two gools the th3rea$on disnals... :) seven twenty one pilots :( sedayne : / sundog -- p!nk![] and "{the queen?}"')) 

