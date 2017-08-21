from ws4py.client.threadedclient import WebSocketClient
import json
import traceback
import datetime
from subprocess import Popen

class GDAX(WebSocketClient):


	def __init__(self, URL):
		WebSocketClient.__init__(self, URL)
		self._newFile()


	def _newFile(self):
		self.filename = '../data/raw/'+datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S.csv')
		self.w = open(self.filename,'w') 
		self.counter = 0

	def opened(self):
		data = json.dumps({
			  "type": "subscribe",
			  "product_ids": ["BTC-USD"]
			})
		print "Starting"
		self.send(data)

	def received_message(self, message):
		self.w.write(str(message)+'\n')


		self.counter+=1
		if self.counter%5000:
			#print "Flushing"
			self.w.flush()


		if self.counter==1000000:
			self.w.close()
			Popen(["gzip",self.filename])
			self._newFile()
			


	def closed(self, code, reason=None):
		self.w.close()
		print "Closed (code",code,") because of",reason
		Popen(["gzip",self.filename])

        def once(self):
                """
                Performs the operation of reading from the underlying
                connection in order to feed the stream of bytes.
                We start with a small size of two bytes to be read
                from the connection so that we can quickly parse an
                incoming frame header. Then the stream indicates
                whatever size must be read from the connection since
                it knows the frame payload length.
                It returns `False` if an error occurred at the
                socket level or during the bytes processing. Otherwise,
                it returns `True`.
                """
                if self.terminated:
                        return False

                try:
                        b = self.sock.recv(self.reading_buffer_size)
                except (socket.error, OSError, pyOpenSSLError) as e:
                        self.unhandled_error(e)
                        return False
                else:
                        if not self.process(b):
                                return False

                return True


		

			

if __name__=="__main__":
	URL = "wss://ws-feed.gdax.com"


	try:
		b = GDAX(URL)
		b.connect()
		b.run_forever()

	except KeyboardInterrupt:
		print "Keyboard interrupt detected."
		b.closed(-1)

	finally:
		traceback.print_exc()
