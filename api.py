from redis import Redis
import time
import os
from datetime import datetime
from flask import request, jsonify, Response, Flask

app = Flask(__name__)
r = Redis(host=os.getenv('REDIS_HOST'), port=int(os.getenv('REDIS_PORT')), decode_responses=True)

# We will have the following auxiliary structures:
# - country_id -> we increment it to get the next country id
# - city_id -> we increment it to get the next city id
# - temp_id -> we increment it to het the next temperature id
# - countries -> set with the names of countries to be sure there are no duplicates
# - country_ids -> set with the ids of all the countries for easier iteration
# - cities -> set with a code formed from the id of the country and the name of
# 		the city to be sure that the country id cobined with the city name are unique
# - city_ids -> set with the ids of all the cities for easier iteration
# - temps -> set with a code formed from the id of the city and the timestamp
# - temp_ids -> set with the ids of all the temperatures for easier iteration

# This will generate the ids uniquely
r.set('country_id', 0)
r.set('city_id', 0)
r.set('temp_id', 0)


def get_new_id(id_type: str) -> int:
	"""The function finds the next unused id and increments the global counter
	Args:
		id_type (str): The name of the type of id we want to get

	Returns:
		int: An unique ID
	"""

	new_id = id_type + '_' + str(r.incr(id_type))

	# If the id already exists, then we skip it
	while len(r.hgetall(new_id)) > 0:
		new_id = id_type + '_' + str(r.incr(id_type))

	return new_id


def decode_id(id: str) -> int:
	"""We keep the ids encoded, so that a city can have the same ID as a
		country for example, but in the database to have 2 separate keys.
		This function retrieves the number that is the ID.

	Args:
		id (str): The ID as it is in the database

	Returns:
		int: The actual ID of the entry
	"""

	return int(id.split('_')[2])


@app.route("/api/countries", methods=["POST"])
def post_country():
	payload = request.get_json()

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'nume' in payload or not 'lat' in payload or not 'lon' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['nume'], str) or not isinstance(payload['lat'], float) or \
		 not isinstance(payload['lon'], float):

		return jsonify({'status': 'BAD REQUEST'}), 400

	# If the country already exists, then we return 409
	if r.sismember('countries', payload['nume']) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	id = get_new_id('country_id')
	r.sadd('countries', payload['nume'])
	r.sadd('country_ids', id)
	r.hset(id, mapping={
		'nume_tara': payload['nume'],
		'latitudine': payload['lat'],
		'longitudine': payload['lon']
	})

	return jsonify({'id': decode_id(id)}), 201


@app.route("/api/countries", methods=["GET"])
def get_country():
	response = []
	country_ids = r.smembers('country_ids')

	for id in country_ids:
		response.append({
			'id': decode_id(id),
			'nume': r.hget(id, 'nume_tara'),
			'lat': float (r.hget(id, 'latitudine')),
			'lon': float (r.hget(id, 'longitudine'))
		})

	return jsonify(response), 200


@app.route("/api/countries/<id>", methods=["PUT"])
def put_country(id):
	payload = request.get_json()

	if not id.isdigit():
		return jsonify({'status': 'NOT FOUND'}), 404

	encoded_id = 'country_id_' + id
	id = int(id)

	if r.sismember('country_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'id' in payload or not 'nume' in payload or not 'lat' in payload or not 'lon' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['id'], int) or not isinstance(payload['nume'], str) or \
		 not isinstance(payload['lat'], float) or not isinstance(payload['lon'], float):

		return jsonify({'status': 'BAD REQUEST'}), 400

	new_encoded_id = 'country_id_' + str(payload['id'])

	# We verify if the new id already exists
	if id != payload['id'] and len(r.hgetall(new_encoded_id)) > 0:
		return jsonify({'status': 'CONFLICT'}), 409

	# If we update the name of the country, we verify if it doesn't already exist
	if payload['nume'] != r.hget(encoded_id, 'nume_tara') and r.sismember('countries', payload['nume']) == 1:
		return jsonify({'status': 'BAD REQUEST'}), 409

	if payload['nume'] != r.hget(encoded_id, 'nume_tara'):
		r.srem('countries', r.hget(encoded_id, 'nume_tara'))
		r.sadd('countries', payload['nume'])

	if id != payload['id']:
		r.srem('country_ids', encoded_id)
		r.sadd('country_ids', new_encoded_id)
		r.hdel(encoded_id, 'nume_tara', 'latitudine', 'longitudine')

	r.hset(new_encoded_id, mapping={
		'nume_tara': payload['nume'],
		'latitudine': payload['lat'],
		'longitudine': payload['lon']
	})

	return jsonify({'status': 'OK'}), 200


@app.route("/api/countries/<id>", methods=["DELETE"])
def del_country(id):
	if not id.isdigit():
		return jsonify({'status': 'BAD REQUEST'}), 400

	encoded_id = "country_id_" + id
	id = int(id)

	if r.sismember('country_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	r.srem('countries', r.hget(encoded_id, 'nume_tara'))
	r.srem('country_ids', encoded_id)
	r.hdel(encoded_id, 'nume_tara', 'latitudine', 'longitudine')

	return jsonify({'status': 'OK'}), 200


@app.route("/api/cities", methods=["POST"])
def post_city():
	payload = request.get_json()

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'idTara' in payload or not 'nume' in payload or not 'lat' in payload or not 'lon' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['idTara'], int) or not isinstance(payload['nume'], str) or \
	   not isinstance(payload['lat'], float) or not isinstance(payload['lon'], float):

		return jsonify({'status': 'BAD REQUEST'}), 400

	if r.sismember('country_ids', 'country_id_' + str(payload['idTara'])) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	# We verify that there are no 2 cities with the same name and the same country ID
	city_code = str(payload['idTara']) + '_' + payload['nume']
	if r.sismember('cities', city_code) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	new_id = get_new_id('city_id')
	r.sadd('cities', city_code)
	r.sadd('city_ids', new_id)
	r.hset(new_id, mapping= {
		'id_tara': payload['idTara'],
		'nume_oras': payload['nume'],
		'latitudine': payload['lat'],
		'longitudine': payload['lon']
	})

	return jsonify({'id': decode_id(new_id)}), 201


@app.route("/api/cities", methods=["GET"])
def get_city():
	response = []
	city_ids = r.smembers('city_ids')

	for city in city_ids:
		response.append({
			'id': decode_id(city),
			'idTara': int(r.hget(city, 'id_tara')),
			'nume': r.hget(city, 'nume_oras'),
			'lat': float(r.hget(city, 'latitudine')),
			'lon': float(r.hget(city, 'longitudine'))
		})

	return jsonify(response), 200


@app.route("/api/cities/country/<id_Tara>", methods=["GET"])
def get_city_by_country_id(id_Tara):
	response = []
	city_ids = r.smembers('city_ids')

	for city in city_ids:
		if r.hget(city, 'id_tara') == id_Tara:
			response.append({
				'id': decode_id(city),
				'idTara': int(r.hget(city, 'id_tara')),
				'nume': r.hget(city, 'nume_oras'),
				'lat': float(r.hget(city, 'latitudine')),
				'lon': float(r.hget(city, 'longitudine'))
			})

	return jsonify(response), 200


@app.route("/api/cities/<id>", methods=["PUT"])
def put_city(id):
	payload = request.get_json()

	if not id.isdigit():
		return jsonify({'status': 'NOT FOUND'}), 404

	encoded_id = 'city_id_' + id
	id = int(id)

	if r.sismember('city_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'id' in payload or not 'idTara' in payload or not 'nume' in payload or \
	   not 'lat' in payload or not 'lon' in payload:

		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['id'], int) or not isinstance(payload['idTara'], int) or \
	   not isinstance(payload['nume'], str) or not isinstance(payload['lat'], float) or \
	   not isinstance(payload['lon'], float):

		return jsonify({'status': 'BAD REQUEST'}), 400

	new_encoded_id = 'city_id_' + str(payload['id'])

	if payload['id'] != id and len(r.hgetall(new_encoded_id)) > 0:
		return jsonify({'status': 'CONFLICT'}), 409

	if r.sismember('country_ids', 'country_id_' + str(payload['idTara'])) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	# If the country ID or the name of the city changed, then we need to verify
	# if the new pair doesn't already exist
	city_code = str(payload['idTara']) + '_' + payload['nume']
	old_city_code = r.hget(encoded_id, 'id_tara') + '_' + r.hget(encoded_id, 'nume_oras')
	if city_code != old_city_code and r.sismember('cities', city_code) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	if payload['id'] != id:
		r.srem('city_ids', encoded_id)
		r.sadd('city_ids', new_encoded_id)
		r.hdel(encoded_id, 'id_tara', 'nume_oras', 'latitudine', 'longitudine')

	if city_code != old_city_code:
		r.srem('cities', old_city_code)
		r.sadd('cities', city_code)

	r.hset(new_encoded_id, mapping= {
		'id_tara': payload['idTara'],
		'nume_oras': payload['nume'],
		'latitudine': payload['lat'],
		'longitudine': payload['lon']
	})

	return jsonify({'status': 'OK'}), 200


@app.route("/api/cities/<id>", methods=["DELETE"])
def del_city(id):
	if not id.isdigit():
		return jsonify({'status': 'BAD REQUEST'}), 400

	encoded_id = 'city_id_' + id
	id = int(id)

	if r.sismember('city_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	city_code = r.hget(encoded_id, 'id_tara') + '_' + r.hget(encoded_id, 'nume_oras')
	r.srem('cities', city_code)
	r.srem('city_ids', encoded_id)
	r.hdel(encoded_id, 'id_tara', 'nume_oras', 'latitudine', 'longitudine')

	return jsonify({'status': 'OK'}), 200


@app.route("/api/temperatures", methods=["POST"])
def post_temperature():
	payload = request.get_json()
	timestamp = time.time()

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'idOras' in payload or not 'valoare' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['idOras'], int) or not isinstance(payload['valoare'], float):
		return jsonify({'status': 'BAD REQUEST'}), 400

	city_id = 'city_id_' + str(payload['idOras'])
	if r.sismember('city_ids', city_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	temp_code = str(payload['idOras']) + '_' + str(timestamp)
	if r.sismember('temps', temp_code) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	id = get_new_id('temp_id')
	r.sadd('temps', temp_code)
	r.sadd('temp_ids', id)
	r.hset(id, mapping={
		'valoare': payload['valoare'],
		'timestamp': timestamp,
		'idOras': payload['idOras']
	})

	return jsonify({'id': decode_id(id)}), 201


@app.route("/api/temperatures", methods=["GET"])
def get_temp():
	response = []
	lat = request.args.get('lat')
	lon = request.args.get('lon')
	from_date = request.args.get('from')
	until_date = request.args.get('until')

	if lat:
		lat = float(lat)

	if lon:
		lon = float(lon)

	if from_date:
		from_date = datetime.strptime(from_date, '%Y-%m-%d')

	if until_date:
		until_date = datetime.strptime(until_date, '%Y-%m-%d')

	for temp_id in r.smembers('temp_ids'):
		city_id = 'city_id_' + str(r.hget(temp_id, 'idOras'))
		timestamp = datetime.fromtimestamp(float(r.hget(temp_id, 'timestamp')))

		if ((lat != None and float(r.hget(city_id, 'latitudine')) == lat) or lat == None) and \
			((lon != None and float(r.hget(city_id, 'longitudine')) == lon) or lon == None) and \
			((from_date != None and timestamp >= from_date) or from_date == None) and \
			((until_date != None and timestamp <= until_date) or until_date == None):

			response.append({
				'id': decode_id(temp_id),
				'valoare': float(r.hget(temp_id, 'valoare')),
				'timestamp': timestamp.strftime('%Y-%m-%d')
			})

	return jsonify(response), 200


@app.route("/api/temperatures/cities/<idOras>", methods=["GET"])
def get_temp_by_city(idOras):
	response = []

	if not idOras.isdigit():
		return jsonify(response), 200

	idOras = int(idOras)
	from_date = request.args.get('from')
	until_date = request.args.get('until')

	if from_date:
		from_date = datetime.strptime(from_date, '%Y-%m-%d')

	if until_date:
		until_date = datetime.strptime(until_date, '%Y-%m-%d')

	for temp_id in r.smembers('temp_ids'):
		if int(r.hget(temp_id, 'idOras')) == idOras:
			timestamp = datetime.fromtimestamp(float(r.hget(temp_id, 'timestamp')))
			if ((from_date != None and from_date <= timestamp) or from_date == None) and \
				((until_date != None and until_date >= timestamp) or until_date == None):

				response.append({
					'id': decode_id(temp_id),
					'valoare': float(r.hget(temp_id, 'valoare')),
					'timestamp': timestamp.strftime('%Y-%m-%d')
				})

	return jsonify(response), 200


@app.route("/api/temperatures/countries/<id_tara>", methods=["GET"])
def get_temp_by_country(id_tara):
	response = []

	if not id_tara.isdigit():
		return jsonify(response), 200

	id_tara = int(id_tara)
	from_date = request.args.get('from')
	until_date = request.args.get('until')

	if from_date:
		from_date = datetime.strptime(from_date, '%Y-%m-%d')

	if until_date:
		until_date = datetime.strptime(until_date, '%Y-%m-%d')

	for temp_id in r.smembers('temp_ids'):
		timestamp = datetime.fromtimestamp(float(r.hget(temp_id, 'timestamp')))

		for city_id in r.smembers('city_ids'):
			if int(r.hget(temp_id, 'idOras')) == decode_id(city_id) and int(r.hget(city_id, 'id_tara')) == id_tara:
				if ((from_date != None and from_date <= timestamp) or from_date == None) and \
					((until_date != None and until_date >= timestamp) or until_date == None):

					response.append({
					'id': decode_id(temp_id),
					'valoare': float(r.hget(temp_id, 'valoare')),
					'timestamp': timestamp.strftime('%Y-%m-%d')
				})

	return jsonify(response), 200


@app.route("/api/temperatures/<id>", methods=["PUT"])
def put_temp(id):
	payload = request.get_json()

	if not id.isdigit():
		return jsonify({'status': 'NOT FOUND'}), 404

	encoded_id = 'temp_id_' + id
	id = int(id)

	if r.sismember('temp_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	if not payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not 'id' in payload or not 'idOras' in payload or not 'valoare' in payload:
		return jsonify({'status': 'BAD REQUEST'}), 400

	if not isinstance(payload['id'], int) or not isinstance(payload['idOras'], int) or \
    	not isinstance(payload['valoare'], float):

		return jsonify({'status': 'BAD REQUEST'}), 400

	city_id = 'city_id_' + str(payload['idOras'])
	if r.sismember('city_ids', city_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	timestamp = r.hget(encoded_id, 'timestamp')
	new_timestamp = time.time()
	temp_code = str(payload['idOras']) + '_' + timestamp
	new_temp_code = str(payload['idOras']) + '_' + str(new_timestamp)
	if temp_code != new_temp_code and r.sismember('temps', new_temp_code) == 1:
		return jsonify({'status': 'CONFLICT'}), 409

	if temp_code != new_temp_code:
		r.srem('temps', temp_code)
		r.sadd('temps', new_temp_code)

	if id != payload['id']:
		r.srem('temp_ids', encoded_id)
		r.sadd('temp_ids', 'temp_id_' + str(payload['id']))
		r.hdel(encoded_id, 'valoare', 'timestamp', 'idOras')

	r.hset(id, mapping={
		'valoare': payload['valoare'],
		'timestamp': new_timestamp,
		'idOras': payload['idOras']
	})

	return jsonify({'status': 'OK'}), 200


@app.route("/api/temperatures/<id>", methods=["DELETE"])
def del_temp(id):
	if not id.isdigit():
		return jsonify({'status': 'BAD REQUEST'}), 400

	encoded_id = 'temp_id_' + id
	id = int(id)

	if r.sismember('temp_ids', encoded_id) == 0:
		return jsonify({'status': 'NOT FOUND'}), 404

	temp_code = r.hget(encoded_id, 'idOras') + '_' + r.hget(encoded_id, 'timestamp')
	r.srem('temps', temp_code)
	r.srem('temp_ids', encoded_id)
	r.hdel(encoded_id, 'valoare', 'timestamp', 'idOras')

	return jsonify({'status': 'OK'}), 200


if __name__ == '__main__':
   app.run('0.0.0.0', debug=True)