#!/usr/bin/python3

import sys
from json import loads, dumps
from time import sleep
import csv
from unidecode import unidecode #Only non native
from subprocess import call, Popen
import concurrent.futures
from urllib.parse import quote_plus, quote
from argparse import ArgumentParser 
from re import sub
from os import makedirs, listdir
from os.path import join, basename, dirname, relpath, abspath, isdir
from difflib import get_close_matches 
from datetime import datetime


parser = ArgumentParser(
	prog="lnac",
	description="link generator and audio compression tool",
	usage="%(prog)s [options]",
	epilog="Made and maintained by @5skr0ll3r",
	)

parser.add_argument('-d', '--dir', help="Path to directory for link generation and file comprassion")
parser.add_argument('-u', '--url', help="Cloud/Local server in which the files are stored. Required to generate the links to the files")
parser.add_argument('-j', '--curl', help="Url where covers for products are stored")
parser.add_argument('-k', '--covers', help="Path to directory containing product covers to be included in the csv")
parser.add_argument('-i', '--csv', help="Path to csv to be associated with the directory provided and generate a new csv containing the download data")
parser.add_argument('-c', '--compress', action="store_true", help="Use this option to compress audio files")
parser.add_argument('-t', '--threads', help="How many threads to run (Default: 20)")

class LNAC_Manager(object):
	def __init__(self, _directory_path=None, _url=None, _curl=None):
		if not _directory_path or not _url:
			sys.exit('LNAC_Manager constructor requires two arguments directory_path and url')
		if not _curl:
			self.covers_url = _url
		else:
			self.covers_url = f"https://athenalibrary.gr/wp-content/uploads/2024/12/"
		self.cloud_front_url = _url
		self.std_path = ".tmp_lnac_std"
		self.final_directory = "compressed_audio_files_128kbps/"
		self.directory_path = _directory_path
		self.file_tree = {}
		self.file_dict = {}
		self.file_list = []
		self.or_file_list = []
		self.threads = 20


	def init(self):
		makedirs( self.std_path, exist_ok=True )
		self.file_tree = { "type": "directory", "name": self.directory_path, "contents": self.directory_crawler( self.directory_path ) } #loads( f"{self.directory_crawler( self.directory_path )}" )
		self.parse_tree()

	def directory_crawler( self, path ):
		if not isdir( path ):
			sys.exit( f"Provided path: {path} is not a directory or does not exist" )
		items = []
		directory_content = sorted( listdir( path ) )
		if not directory_content:
			with open( join( self.std_path, 'empty_folders.lst' ), 'w' ) as ef:
				ef.write( f"Path: {path} is empty")
				return []
		for entry in directory_content:
			if entry.startswith('.'):
				continue

			full_path = join( path, entry )

			if isdir( full_path ):
				items.append({
					"type": "directory",
					"name": f"{entry}",
					"contents": self.directory_crawler( full_path ) or []
					})
			else:
				items.append({
					"type": "file",
					"name": f"{entry}"
					})
		return items


	#tree -xRiJ compressed_audio_files_128kbps/ > audio_files_tree.json
	def generate_tree(self) -> str:
		with open( join(self.std_path, '.tmp_stdout' ), 'w' ) as fout:
			call( [ 'tree', '-xRiJ', self.directory_path ], stdout=fout, universal_newlines=True )
			fout.seek(0)
			output = fout.read()
			return output


	def parse_tree(self):
		for i in self.file_tree["contents"]:
			if i['type'] == 'directory' and i["contents"]:
				self.file_dict[i['name']] = {"product_code": i['name'].split('.')[0].split("_")[0], "cover": "", "links": []}
				for x in i['contents']:
					if x['type'] == 'directory' and x["contents"]:
						for y in x['contents']:
							path = f"{self.final_directory}{i['name']}/{x['name']}/{y['name']}"
							c_path = f"{self.directory_path}{i['name']}/{x['name']}/{y['name']}"
							self.or_file_list.append( c_path )
							self.file_list.append( path )
							self.file_dict[i['name']]['links'].append( path )
					else:
						path = f"{self.final_directory}{i['name']}/{x['name']}"
						c_path = f"{self.directory_path}{i['name']}/{x['name']}"
						self.or_file_list.append( c_path )
						self.file_list.append( path )
						self.file_dict[i['name']]['links'].append( path )

		with open( join( self.std_path, "non_mp3_files.lst" ), 'w' ) as nmf:
			for j in self.file_list:
				if ".mp3" not in j.lower():
					nmf.write(f"{j}\n")
		

	def compress_audio_files(self):
		files_to_process = list( self.or_file_list )
		if not files_to_process:
			print("❌ No files to compress!")


		print(f"🔍 Found {len( files_to_process )} files to compress")
		with concurrent.futures.ThreadPoolExecutor( max_workers=self.threads ) as executor:
			futures = []
			for i, file in enumerate( files_to_process ):
				future = executor.submit( self.worker, i, file, self.directory_path )
				futures.append( future )

			concurrent.futures.wait( futures )
			
		print("✅ Compression complete!")


	def worker( self, worker_id, file_path, base_input_dir ):
		file_path = abspath( file_path )
		relative_path = relpath( file_path, base_input_dir )

		output_file = self.file_list[worker_id]
		try:
			makedirs( dirname( output_file ), exist_ok=True )
		except Exception as e:
			print(f"❌ Failed to create directory: {dirname( output_file )}")
			print(f"Error: {e}")
			return

		log_stdout = join( self.std_path, f'.tmp_stdout_{worker_id}' )
		log_stderr = join( self.std_path, f'.tmp_stderr_{worker_id}' )
		print(f"🚀 Running: ffmpeg -i {file_path} -b:a 128k -ar 44100 -ac 2 -y {output_file}")

		with open( log_stdout, 'w' ) as fstoud, open( log_stderr, 'w' ) as fstderr :
			print( f"🚀 Worker {worker_id} started for {file_path} \nOutpath: {output_file}" )
			result = call( [ 'ffmpeg', '-i', file_path, '-b:a', '128k', '-ar', '44100', '-ac', '2', '-y', output_file ], stdout=fstoud, stderr=fstderr )
		if result != 0:
			print( f"❌ Error processing {file_path}! Check {log_stderr}" )
		print( f"✅ Worker {worker_id} finished processing {file_path}" )


	def associate_covers( self, covers_path ):
		if( not isdir( covers_path ) ):
			print( f"Path {covers_path} was not found or is not a directory" )
			sys.exit(1)
		covers = listdir( covers_path )
		for index, path in enumerate(covers):
			file_code = basename( path ).split('.')[0]
			for i, product in enumerate( self.file_dict ):
				if self.file_dict[product]['product_code'] == file_code:
					noramlize = str.maketrans({ " ": "-", ",": "", "/": "", "'": "", "!": "", "&": ""})
					self.file_dict[product]['cover'] = self.covers_url + sub( r'-+', '-', path.translate( noramlize ) )#.replace( "   ", "-" ).replace( "  ", "-" ).replace( " ", "-" ).replace(",/'", "")


	def do_csv( self, csv_path, covers_path="" ):
		with open( csv_path, 'r', encoding="utf-8" ) as original, open( 'new_csv.csv', 'w', encoding="utf-8", newline="" ) as noriginal, open( join( self.std_path, 'csv_report.log'), 'w' ) as reporter:
			csv_reader = csv.DictReader( original, delimiter=',' )
			fieldnames = csv_reader.fieldnames[:]
			rows = list( csv_reader )
			if covers_path:
				self.associate_covers( covers_path )

			max_downloads = 0
			mod_file_name_list = [ a.split( '_' )[ -1 ] for a in self.file_dict.keys() ]
			unmod_file_name_list = list( self.file_dict.keys() )

			for row in rows:
				product_name = row['Name']
				sort_product_name = product_name.split(' /')[0].split(' :')[0].split('(')[0].split('[')[0]
				close_matches = get_close_matches( sort_product_name, mod_file_name_list )

				if close_matches:
					for product in unmod_file_name_list:
						if close_matches[0] in product:
							product = self.file_dict.get(product, [])
							max_downloads = max( max_downloads, len( product['links'] ) )
				else:
					reporter.write( f"No match for product: {sort_product_name} was found\n" )
			for i in range( 1, max_downloads + 1 ):
				fieldnames.append( f"Download {i} ID" )
				fieldnames.append( f"Download {i} name" )
				fieldnames.append( f"Download {i} URL" )

			for row in rows:
				product_name = row['Name']
				sort_product_name = product_name.split( ' /' )[0].split( ' :' )[0].split( '(' )[0].split( '[' )[0]
				close_matches = get_close_matches( sort_product_name, mod_file_name_list )

				if close_matches:
					for product in unmod_file_name_list:
						if close_matches[0] in product:
							product = self.file_dict.get(product, [])
							for i, path in enumerate(product['links'], start=1):

								if path.count( "/" ) > 2:
									file_name = path.split('/')[-1:]
									file_name = '/'.join( file_name ).replace( "/", "_" )
								else:
									file_name = path.split('/')[-1]

								link = self.cloud_front_url + quote( path )
								print(link)
								if product['cover']:
									row[f"Images"] = product['cover']
								else:
									reporter.write( f"No Cover for product: {sort_product_name} was found\n" )
								row[f"Download {i} ID"] = f"ID-{i}"
								row[f"Download {i} name"] = file_name
								row[f"Download {i} URL"] = link

				for field in fieldnames:
					if field not in row:
						row[field] = ""

			writer = csv.DictWriter( noriginal, fieldnames=fieldnames )
			writer.writeheader()
			writer.writerows( rows )

def main():
	args = parser.parse_args()
	if( not args.dir ):
		parser.print_help( sys.stderr )
		sys.exit( 1 )

	lnac = LNAC_Manager( args.dir, args.url, args.curl )
	lnac.init()

	if( args.compress ):
		
		lnac.threads = int( args.threads ) if args.threads else lnac.threads
		print( f"Compressing with {lnac.threads} threads" )
		lnac.compress_audio_files()
	if( args.csv ):
		print("Generating new_csv.csv")
		lnac.do_csv( args.csv, args.covers or "" )

	else:
		parser.print_help( sys.stderr )
		sys.exit( 0 )

if __name__ == '__main__':
	main()


#Images: 30