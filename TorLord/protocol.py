import asyncio
import logging
import struct
from asyncio import Queue
from concurrent.futures import CancelledError

import bitstring

# The default request size for blocks of pieces is 2^14 bytes.
#       https://wiki.theory.org/BitTorrentSpecification
#
REQUEST_SIZE = 2**14


class ProtocolError(BaseException):
    pass


class PeerConnection:
    def __init__(self, queue: Queue, info_hash,
                 peer_id, piece_manager, on_block_cb=None):
        """
        :param queue: The async Queue containing available peers
        :param info_hash: The SHA1 hash for the meta-data's info
        :param peer_id: Our peer ID used to to identify ourselves
        :param piece_manager: The manager responsible to determine which pieces
                              to request
        :param on_block_cb: The callback function to call when a block is
                            received from the remote peer
        """
        self.my_state = []
        self.peer_state = []
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_id = None
        self.writer = None
        self.reader = None
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.future = asyncio.ensure_future(self._start())  # Start this worker-->worker is basically like a client

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = await self.queue.get()
            logging.info('Got assigned peer with: {ip}'.format(ip=ip))

            try:
                self.reader, self.writer = await asyncio.open_connection(
                    ip, port)
                logging.info('Connection open to peer: {ip}'.format(ip=ip))
                buffer = await self._handshake()
                # The default state for a connection is that peer is not
                # interested and we are choked
                self.my_state.append('choked')

                # Let the peer know we're interested in downloading pieces
                await self._send_interested()
                self.my_state.append('interested')

                # Start reading responses as a stream of messages for as
                # long as the connection is open and data is transmitted
                async for message in PeerStreamIterator(self.reader, buffer):
                    if 'stopped' in self.my_state:
                        break
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id,
                                                    message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append('choked')
                    elif type(message) is Unchoke:
                        if 'choked' in self.my_state:
                            self.my_state.remove('choked')
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id,
                                                       message.index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove('pending_request')
                        self.on_block_cb(
                            peer_id=self.remote_id,
                            piece_index=message.index,
                            block_offset=message.begin,
                            data=message.block)
                    elif type(message) is Request:
                        logging.info('Ignoring the received Request message.')
                    elif type(message) is Cancel:
                        logging.info('Ignoring the received Cancel message.')

                    # Send block request to remote peer if we're interested
                    if 'choked' not in self.my_state:
                        if 'interested' in self.my_state:
                            if 'pending_request' not in self.my_state:
                                self.my_state.append('pending_request')
                                await self._request_piece()

            except Protoc