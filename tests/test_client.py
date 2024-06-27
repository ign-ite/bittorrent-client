import unittest

from . import no_logging
from TorLord.client import Piece, Block


class PieceTests(unittest.TestCase):
    def test_empty_piece(self):
        p = Piece(0, blocks=[], hash_value=None)
        self.assertIsNone(p.next_request())

    def test_request_ok(self):
        blocks = [Block(0, offset, length=10) for offset in range(0, 100, 10)]
        p = Piece(0, blocks, hash_value=None)

        block = p.next_request()
        missing_blocks = [b for b in p.blocks if b.status is Block.Missing]
        pending_blocks = [b for b in p.blocks if b.status is Block.Pending]

        self.assertEqual(1, len(pending_blocks))
        self.assertEqual(9, len(missing_blocks))
        self.assertEqual(block, pending_blocks[0])

    def test_reset_missing_block(self):
        p = Piece(0, blocks=[], hash_value=None)
        with no_logging:
            p.block_received(123, b'')   # Should not throw

    def test_reset_block(self):
        blocks = [Block(0, offset, length=10) for offset in range(0, 100, 10)]
        p = Piece(0, blocks, hash_value=None)

        p.block_received(10, b'')

        self.assertEqual(1, len([b for b in p.blocks
                                if b.status is Block.Retrieved]))
        self.assertEqual(9, len([b for b in p.blocks
                                if b.status is Block.Missing]))