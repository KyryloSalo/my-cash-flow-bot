import unittest
from photo_pipeline import plan_vertical_tiles


class PhotoTileTests(unittest.TestCase):
    def test_vertical_tiles_overlap_and_cover_long_image(self):
        self.assertEqual(plan_vertical_tiles(height=1000, tile_height=400, overlap=0.2),
                         ((0, 400), (320, 720), (600, 1000)))

    def test_invalid_geometry(self):
        with self.assertRaises(ValueError):
            plan_vertical_tiles(height=100, tile_height=0, overlap=0.2)

    def test_small_image_uses_one_tile(self):
        self.assertEqual(plan_vertical_tiles(height=100, tile_height=400), ((0, 100),))
