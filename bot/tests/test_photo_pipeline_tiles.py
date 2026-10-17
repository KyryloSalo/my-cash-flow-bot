import unittest
from photo_pipeline import plan_vertical_tiles, assess_image_quality
from io import BytesIO
from PIL import Image


class PhotoTileTests(unittest.TestCase):
    def test_vertical_tiles_overlap_and_cover_long_image(self):
        self.assertEqual(plan_vertical_tiles(height=1000, tile_height=400, overlap=0.2),
                         ((0, 400), (320, 720), (600, 1000)))

    def test_invalid_geometry(self):
        with self.assertRaises(ValueError):
            plan_vertical_tiles(height=100, tile_height=0, overlap=0.2)

    def test_small_image_uses_one_tile(self):
        self.assertEqual(plan_vertical_tiles(height=100, tile_height=400), ((0, 100),))

    def test_quality_signals_are_deterministic_and_non_secret(self):
        buf = BytesIO()
        Image.new("RGB", (40, 80), "white").save(buf, format="PNG")
        quality = assess_image_quality(buf.getvalue())
        self.assertEqual((quality.width, quality.height), (40, 80))
        self.assertGreaterEqual(quality.glare_ratio, 0)
        self.assertEqual(quality, assess_image_quality(buf.getvalue()))
