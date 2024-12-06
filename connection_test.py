import socketio
import time

sio = socketio.Client()

token = 'yqjdHAvNM2y6kB2r2Lq7PLEXEtXTnPF6'


@sio.event
def connect():
    print("Connected to server")

@sio.event
def data(event):

    print("Received data:**********")


@sio.event
def disconnect():
    print("Disconnected from server")

TOKEN = 'yqjdHAvNM2y6kB2r2Lq7PLEXEtXTnPF6'
URL = 'http://localhost:3001'

def main():
    """Main function to connect to the server and maintain the connection."""


    
    try:
        sio.connect(f'{URL}?token={TOKEN}', transports='websocket')
        while True:

            logging.debug("Running...")

    except Exception as e:
        logging.error(f"Connection failed: {e}")
    except KeyboardInterrupt:
        logging.info("Exiting...")
        sio.disconnect()

if __name__ == "__main__":
    main()

