import aiohttp
import random
import logging
import socket
from struct import unpack
from urllib.parse import urlencode

import bencoding


class Tracker:
    """
    Represents the connection to a tracker for a given Torrent that is either
    under download or seeding state.
    """

    def __init__(self, torrent):
        self.torrent = torrent
        self.peer_id = _calculate_peer_id()
        self.http_client = aiohttp.ClientSession()

    async def connect(self,
                      first: bool = None,
                      uploaded: int = 0,
                      downloaded: int = 0):
        """
        Makes the announce call to the tracker to update with our statistics
        as well as get a list of available peers to connect to. --> Understood

        If the call was successful, the list of peers will be updated as a
        result of calling this function. --> Pretty Clear

        :param first: Whether or not this is the first announce call }--> Ok ok understanding
        :param uploaded: The total number of bytes uploaded
        :param downloaded: The total number of bytes downloaded
        """
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': self.torrent.total_size - downloaded,
            'compact': 1}
        if first:
            params['event'] = 'started'

        url = self.torrent.announce + '?' + urlencode(params)
        logging.info('Connecting to tracker at: ' + url)

        async with self.http_client.get(url) as response:
            if not response.status == 200:
                raise ConnectionError('Unable to connect to tracker: status code {}'.format(response.status))
            data = await response.read()
            self.raise_for_error(data)
            return TrackerResponse(bencoding.Decoder(data).decode()) #Tracker Response is what im learning rn!

    def close(self):
        self.http_client.close()

    def raise_for_error(self, tracker_response):
        """
        A (hacky) fix to detect errors by tracker even when the response has a status code of 200
        """
        try:
            # a tracker response containing an error will have a utf-8 message only.
            # see: https://wiki.theory.org/index.php/BitTorrentSpecification#Tracker_Response
            message = tracker_response.decode("utf-8")
            if "failure" in message:
                raise ConnectionError('Unable to connect to tracker: {}'.format(message))

        # a successful tracker response will have non-uncicode data, so it's a safe to bet ignore this exception.
        except UnicodeDecodeError:
            pass

    def _construct_tracker_parameters(self):
        """
        Constructs the URL parameters used when issuing the announce call
        to the tracker.
        """
        return {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'port': 6889,
            # TODO Update stats when communicating with tracker
            'uploaded': 0,
            'downloaded': 0,
            'left': 0,
            'compact': 1}


def _calculate_peer_id():
        #Everything about this is written at https://wiki.theory.org/BitTorrentSpecification#peer_id --> Ik You'll forget about this Varun
    return '-PC0001-' + ''.join(
        [str(random.randint(0, 9)) for _ in range(12)])


def _decode_port(port):
    """
    Converts a 32-bit packed binary port number to int
    """
    # Convert from C style big-endian encoded as unsigned short
    return unpack(">H", port)[0]

class TrackerResponse:



