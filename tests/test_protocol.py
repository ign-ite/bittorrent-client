import unittest

from TorLord.protocol import PeerStreamIterator, Handshake, Have, Request, \
    Piece, Interested, Cancel


class PeerStreamIteratorTests(unittest.TestCase):
    def test_parse_empty_buffer(self):
        iterator = PeerStreamIterator(None)
        iterator.buffer = ""
        self.assertIsNone(iterator.parse())


class HandshakeTests(unittest.TestCase):
    def test_construction(self):
        handshake = Handshake(
            info_hash=b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01",
            peer_id=b"-qB3200-iTiX3rvfzMpr")

        self.assertEqual(
            handshake.encode(),
            b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00"
            b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01"
            b"-qB3200-iTiX3rvfzMpr")

    def test_parse(self):
        handshake = Handshake.decode(
            b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00"
            b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01"
            b"-qB3200-iTiX3rvfzMpr")

        self.assertEqual(
            b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01",
            handshake.info_hash)
        self.assertEqual(
            b"-qB3200-iTiX3rvfzMpr",
            handshake.peer_id)


class HaveMessageTests(unittest.TestCase):
    def test_can_construct_have(self):
        have = Have(33)
        self.assertEqual(
            have.encode(),
            b"\x00\x00\x00\x05\x04\x00\x00\x00!")

    def test_can_parse_have(self):
        have = Have.decode(b"\x00\x00\x00\x05\x04\x00\x00\x00!")
        self.assertEqual(33, have.index)