import unittest

from TorLord.torrent import Torrent


class UbuntuTorrentTests(unittest.TestCase):
    def setUp(self):
        self.t = Torrent('tests/data/ubuntu-16.04-desktop-amd64.iso.torrent')

    def test_instantiate(self):
        self.assertIsNotNone(self.t)

    def test_is_single_file(self):
        self.assertFalse(self.t.multi_file)

    def test_announce(self):
        self.assertEqual(
            'http://torrent.ubuntu.com:6969/announce', self.t.announce)

    def test_piece_length(self):
        self.assertEqual(
            524288, self.t.piece_length)

    def test_file(self):
        self.assertEqual(1, len(self.t.files))
        self.assertEqual(
            'ubuntu-16.04-desktop-amd64.iso', self.t.files[0].name)
        #this is the ubuntu iso directory I have the amd one cos i have amd processor!
        self.assertEqual(1485881344, self.t.files[0].length)

    def test_hash_value(self):
        self.assertEqual(
            b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01",
            self.t.info_hash)

    def test_total_size(self):
        self.assertEqual(1485881344, self.t.total_size)

    def test_pieces(self):
        self.assertEqual(2835, len(self.t.pieces))


class SXSWTorrentTests(unittest.TestCase):
    def test_instantiate(self):
        with self.assertRaises(RuntimeError):
            Torrent('tests/data/SXSW_2016_Showcasing_Artists_Part1.torrent')