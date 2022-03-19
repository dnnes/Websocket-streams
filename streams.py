import asyncio
import asyncio.exceptions
import json
import logging
import multiprocessing
import websockets
import websockets.exceptions

#from threading import *
from multiprocessing import Pipe, Process, Queue, Manager
from time import sleep

class GetStream:
    
    def __init__(self, uri:str, subscribe:list = None, queue:Pipe= None) -> None:
        self.wss_url = uri
        self.channels = subscribe
        self.q = queue

    #event loop entry point    
    def initiate(self) -> None:    
        try:
            asyncio.run(self.connect_ws(self.wss_url, self.channels))
        except KeyboardInterrupt:
            logging.info('Closed by Ctrl-c. Bye!')
        except:
            logging.error('The main loop was closed. Retrying to connect soon')
            sleep(5)
            asyncio.run(self.connect_ws(self.wss_url, self.channels))


   
    #open websocket connection, subscribe to channels, create and await main tasks (messages handler and send ping)
    async def connect_ws(self, wss_url, channels) -> None:

        logging.info('Trying connect to server.')
        
        async for ws in websockets.connect(wss_url, open_timeout=5):
            
            logging.info('Connection is open. Proceeding to \'subscribe\' function')
            if not channels == None: await self.subscribe(ws, channels)
            
            #await ws.send(json.dumps(subscribe))
            #await ws.send(json.dumps({"event": "bts:subscribe","data": {"channel": "live_orders_btcusd"}}))
            #await ws.send(json.dumps({"event": "bts:subscribe","data": {"channel": "live_trades_btcusd"}}))
            #print(await ws.recv())
            
            logging.info('Creating tasks')
            stream_thread = await asyncio.to_thread(self.stream_handler, ws)  
            still_alive = asyncio.create_task(self.get_echo(ws), name = 'ping')
            
            logging.info('Initiating tasks')
            try:
                await asyncio.gather(stream_thread, still_alive)
            except Exception as e:
                print(e)
                logging.error('An exception was caught. Maybe the connection was lost. Will iteratively retry connect to server')
                pass
    
    #iterates over all elements in channels list and send a json request
    async def subscribe(self, ws:websockets, channels:list) ->None:
        for c in channels:
            try:
                logging.info(f'sending {c}')
                await ws.send(json.dumps(c))
                resp = await ws.recv()
                print(resp)
                logging.info(f'response: {resp}')
            except:
                pass
        return
        
    #Receives the data stream from multiple channels,
    #any db or message passing logic should be implemented here.
    #Remainder: async for... is a wrapper around common recv() loop
    async def stream_handler(self, ws:websockets) ->None:
        logging.info('Stream handler thread set.')
        async for message in ws:
            #pass
            #print(message)
            self.q.send(message)

    
    async def get_echo(self, ws:websockets) -> None:
        logging.info('Initiating ping task')
        while True:
            try:
                pong = await ws.ping()
                logging.info('PING sent')

                await asyncio.wait_for(pong, 2)
                logging.info('PONG catch')     

                await asyncio.sleep(1)
            except asyncio.exceptions.TimeoutError:
                logging.error('Ping can\'t get response due high latency. TimeoutError will rise to next level')
                raise
            
            
def to_db():
    logging.info("enter todb")
    while True:
        print(p.recv())
    

if __name__ == "__main__":

    #loggin.DEBUG will print EVERY message in stream
    logging.basicConfig(
        filename='scrap.log',
        format="%(asctime)s %(message)s",
        level=logging.INFO)
    
    #this function get elements from queue and send to 
    #apropriate data base
    manager = multiprocessing.Manager()
    p, q = Pipe()
    
    db_process = Process(target=to_db)
    db_process.start()
    

    channels = [{"event": "bts:subscribe","data": {"channel": "live_orders_btcusd"}}]   

    bitstamp = GetStream("wss://ws.bitstamp.net", channels, q)
    bitstamp.initiate()
    



#subs = [{"event": "bts:subscribe","data": {"channel": "live_orderess_"+i}}for i in ['btcusd','ethusd', 'btcusdt', 'ethusdt', 'xrpusdt']]