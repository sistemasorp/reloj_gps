import select, socket, re, datetime, sys, signal, sqlite3
from sqlite3 import Error

RUTA_BBDD = '/tmp/reloj_gps.sqlite3'
PUERTO_ESCUCHA = 8001
servidor = None

# Devuelve la fecha y hora actual
def actual():
	return str(datetime.datetime.now())

# Muestra el mensaje con la fecha y hora actuales
def registra(cadena):
	print(f"{actual()} {cadena}")
	
# Abre la BBDD y crea la tabla si no existe
def conecta_bbdd():
	conexion_bbdd = None
	try:
		conexion_bbdd = sqlite3.connect(RUTA_BBDD)
		cursor = conexion_bbdd.cursor()
		# Si no existen la tablas, se crean
		cursor.execute("CREATE TABLE IF NOT EXISTS dispositivo(identificador TEXT PRIMARY KEY, fecha_vivo TEXT, bateria INTEGER, fecha_ubicacion TEXT, latitud REAL, longitud REAL)")
		registra('Conexión a base de datos establecida.')
	except Error as e:
		registra(e)
	finally:
		return conexion_bbdd
		
# Crea el socket de escucha
def lanza_servidor():
	global servidor
	try:
		servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		servidor.bind(('0.0.0.0', PUERTO_ESCUCHA))
		servidor.listen(5)
		registra(f"Servidor escuchando en puerto el puerto TCP {PUERTO_ESCUCHA}.")
	except Error as e:
		servidor = None
		registra(e)

# Realiza una salida limpia si se pulsa CTRL + C
def salida(sig, frame):
	registra("Saliendo...")
	if not servidor is None:
		servidor.shutdown(socket.SHUT_RDWR)
		servidor.close()
	sys.exit(0)

# Interpreta los comandos recibidos y actua en consecuencia	
def logica_servidor(conexion_bbdd, servidor):
	entradas = [servidor]
	salidas = []
	dispositivos = {}
	
	registra('Esperando conexiones...')
	while True:
		leibles, escribibles, excepciones = select.select(entradas, salidas, entradas, 1)
		for leible in leibles:
			# Si el servidor tiene una conexión pendiente
			if leible is servidor:
				conexion_cliente, direccion = leible.accept()
				registra(f"Conexion establecida desde {direccion}.")
				entradas.append(conexion_cliente)
			# Si es un reloj o cliente que ha enviado datos
			else:
				recepcion = leible.recv(1024)
				# Si hay datos que procesar
				if recepcion:
					datos = recepcion.decode('ascii')
					# Si es un mensaje de link remains
					if '*LK' in datos:
						procesa_link_remains(conexion_bbdd, leible, dispositivos, datos)
					# Si es un mensaje de confirmación de location data reporting
					elif '*CR' in datos:
						procesa_confirmacion(datos)
					# Si es un mensaje de location data reporting
					elif '*UD' in datos:
						procesa_location_data_reporting(conexion_bbdd, datos)
					# Si es un mensaje para activar el envío de la localización
					elif datos.startswith("ACTIVA"):
						procesa_activacion(conexion_bbdd, leible, dispositivos, datos)
					# Si es un mensaje para listar los dispositivos activos
					elif datos == "LISTA":
						procesa_listado(conexion_bbdd, leible, dispositivos, datos)
					# Si es un mensaje para mostrar información de un dispositivo
					elif datos.startswith("INFO"):
						procesa_informacion(conexion_bbdd, leible, dispositivos, datos)
					# Si es un retorno de carro y/o salto de lína
					elif datos.startswith("\r\n"):
						continue
					else:
						registra(f"Mensaje desconocido: {datos}")
				# Si se ha cerrado la conexión del reloj o cliente
				else:
					id_reloj = ""
					for identificador in list(dispositivos):
						if dispositivos[identificador] == leible:
							id_reloj = f" {identificador}"
							del dispositivos[identificador]
					entradas.remove(leible)
					remoto, puerto = leible.getpeername()
					registra(f"Conexión con {remoto}{id_reloj} cerrada.")
					leible.close()
			
# Procesamiento de un mensaje "reloj link remains" del reloj
def procesa_link_remains(conexion_bbdd, conexion_cliente, dispositivos, datos):
	mensaje_link_remains = r"\[(\w{2}\*\w+)\*\w{4}\*LK([\d,]*)\]"
	link_remains = re.match(mensaje_link_remains, datos)
	# Si hay datos formateados correctamente
	if link_remains:
		bateria = 0
		identificador = f"{link_remains.group(1)}"
		#Se envia la reespuesta
		respuesta = f"[{identificador}*0002*LK]"
		conexion_cliente.send(respuesta.encode('ascii'))
		if len(link_remains.group(2)) > 0:
			bateria = link_remains.group(2).split(',')[3]
		registra(f"Mensaje link remains de {identificador}. Batería: {bateria}%")
		cursor = conexion_bbdd.cursor()
		cursor.execute(f"INSERT INTO dispositivo(identificador, fecha_vivo, bateria) VALUES(?, datetime('now'), ?) ON CONFLICT(identificador) DO UPDATE SET fecha_vivo = excluded.fecha_vivo, bateria = excluded.bateria", (identificador, bateria))
		conexion_bbdd.commit()
		if identificador not in dispositivos:
			dispositivos[identificador] = conexion_cliente
			
# Procesamiento de una mensaje "location data reporting" del reloj
def procesa_location_data_reporting(conexion_bbdd, datos):
	mensaje_location_data_reporting = r"\[(\w{2}\*\w+)\*\w{4}\*UD\d?,\d{6},\d{6},(\w),([\s\d\.]+),(\w),([\s\d\.]+),(\w),([\d\w\s\.\-_\:,]+)\]"
	location_data_reporting = re.match(mensaje_location_data_reporting, datos)
	# Si hay datos formateados correctamente
	if location_data_reporting:
		# Si hay posicion GPS válida
		if location_data_reporting.group(2) == 'A':
			identificador = f"{location_data_reporting.group(1)}"
			latitud = f"{'-' if location_data_reporting.group(4) == 'S' else ''}{location_data_reporting.group(3).strip()}"
			longitud = f"{'-' if location_data_reporting.group(6) == 'W' else ''}{location_data_reporting.group(5).strip()}"
			registra(f"Mensaje location data reporting de {identificador}. Latitud: {latitud} Longitud: {longitud}")
			cursor = conexion_bbdd.cursor()
			cursor.execute(f"UPDATE dispositivo SET fecha_ubicacion = datetime('now'), latitud = ?, longitud = ? WHERE identificador = ?", (latitud, longitud, identificador))
			conexion_bbdd.commit()
			
#Procesamiento de un mensaje "ACTIVA" de un usuario o aplicación
def procesa_activacion(conexion_bbdd, conexion_cliente, dispositivos, datos):
	mensaje_activacion = r"ACTIVA (\w{2}\*\w+)"
	activacion = re.match(mensaje_activacion, datos)
	# Si hay datos formateados correctamente
	if activacion:
		identificador = f"{activacion.group(1)}"
		for dispositivo in list(dispositivos):
			if dispositivo == identificador:
				respuesta = f"[{identificador}*0002*CR]"
				dispositivos[identificador].send(respuesta.encode('ascii'))
				conexion_cliente.send(b'ACTIVADO\r\n')

#Procesamiento del mensaje de respuesta del reloj al comando "ACTIVA"
def procesa_confirmacion(datos):
	mensaje_confirmacion = r"\[(\w{2}\*\w+)\*\w{4}\*CR\]"
	confirmacion = re.match(mensaje_confirmacion, datos)
	# Si hay datos formateados correctamente
	if confirmacion:
		identificador = f"{confirmacion.group(1)}"
		registra(f"Activación del envío de la localización para el dispositivo {identificador}.")

#Procesamiento de un mensaje "LISTA" de un usuario o aplicación		
def procesa_listado(conexion_bbdd, conexion_cliente, dispositivos, datos):
	mensaje_listado = r"LISTA"
	listado = re.match(mensaje_listado, datos)
	# Si hay datos formateados correctamente
	if listado:
		registra('Listado de dispositivos.')
		cursor = conexion_bbdd.cursor()
		cursor.execute(f"SELECT identificador FROM dispositivo")
		filas = cursor.fetchall()
		for fila in filas:
			respuesta = f"{fila[0]}\r\n"
			conexion_cliente.send(respuesta.encode('ascii'))
		conexion_cliente.send(b'EOF\r\n')

#Procesamiento de un mensaje "INFO" de un usuario o aplicación
def procesa_informacion(conexion_bbdd, conexion_cliente, dispositivos, datos):
	mensaje_informacion = r"INFO (\w{2}\*\w+)"
	informacion = re.match(mensaje_informacion, datos)
	# Si hay datos formateados correctamente
	if informacion:
		identificador = f"{informacion.group(1)}"
		registra(f"Información del dispositivo {identificador}.")
		cursor = conexion_bbdd.cursor()
		cursor.execute(f"SELECT identificador, fecha_vivo, bateria, fecha_ubicacion, latitud, longitud FROM dispositivo WHERE identificador = ?", (identificador,))
		fila = cursor.fetchone()
		if fila:	
			respuesta = f"{fila[0]},{fila[1]},{fila[2]},{fila[3]},{fila[4]},{fila[5]}\r\n"
			conexion_cliente.send(respuesta.encode('ascii'))
		conexion_cliente.send(b'EOF\r\n')


conexion_bbdd = conecta_bbdd()
if conexion_bbdd is None:
	registra('Hubo problemas con la base de datos. Saliendo...')
	sys.exit(1)
else:
	lanza_servidor()
	if servidor is None:
		registra('Hubo problemas con el servidor. Saliendo...')
		sys.exit(2)
	else:
		signal.signal(signal.SIGINT, salida)
		logica_servidor(conexion_bbdd, servidor)

	
