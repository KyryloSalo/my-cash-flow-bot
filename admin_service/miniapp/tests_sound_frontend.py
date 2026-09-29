from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path


class SoundFrontendContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.base = Path(__file__).parent
        cls.template = (cls.base / "templates" / "miniapp" / "index.html").read_text(encoding="utf-8")
        cls.views = (cls.base / "views.py").read_text(encoding="utf-8")
        cls.sound_path = cls.base / "static" / "miniapp" / "sound-manager.js"
        cls.sound_source = cls.sound_path.read_text(encoding="utf-8")
        cls.push_source = (cls.base / "static" / "miniapp" / "push.js").read_text(encoding="utf-8")
        cls.sound_dir = cls.base / "static" / "miniapp" / "sounds"
        cls.registry = json.loads((cls.sound_dir / "LICENSE.json").read_text(encoding="utf-8"))

    def _function_source(self, name: str, next_name: str) -> str:
        start = self.template.index(f"function {name}")
        end = self.template.index(f"function {next_name}", start)
        return self.template[start:end]

    def test_sound_manager_is_versioned_and_loaded_before_feature_modules(self) -> None:
        sound_tag = "{% static 'miniapp/sound-manager.js' %}?v={{ miniapp_asset_version }}"
        self.assertIn(sound_tag, self.template)
        self.assertLess(self.template.index(sound_tag), self.template.index("{% static 'miniapp/achievements.js' %}"))
        self.assertLess(self.template.index(sound_tag), self.template.index("{% static 'miniapp/push.js' %}"))
        self.assertIn('_miniapp_asset_stamp("sound-manager.js")', self.views)
        self.assertIn('sound_manager_url = _versioned_static("miniapp/sound-manager.js")', self.views)
        self.assertIn('"{sound_manager_url}",', self.views)
        self.assertIn('data-sound-base="{% static \'miniapp/sounds/\' %}"', self.template)
        self.assertIn('data-asset-version="{{ miniapp_asset_version }}"', self.template)

    def test_settings_expose_independent_audio_haptic_controls(self) -> None:
        self.assertIn('id="soundEnabledSetting"', self.template)
        self.assertIn('id="achievementSoundsEnabledSetting"', self.template)
        self.assertIn('id="hapticsEnabledSetting"', self.template)
        self.assertIn('id="soundPreviewButton"', self.template)
        self.assertIn('id="soundSettingsStatus"', self.template)
        self.assertNotIn('id="soundVolumeSetting"', self.template)
        self.assertIn('vydno.sound.enabled', self.sound_source)
        self.assertIn('vydno.sound.achievements', self.sound_source)
        self.assertIn('vydno.haptics.enabled', self.sound_source)

    def test_financial_success_sounds_only_follow_committed_confirmations(self) -> None:
        cases = (
            ("confirmTransactionDraft", "cancelTransactionDraft"),
            ("confirmAccountDraft", "cancelAccountDraft"),
            ("confirmSavingTaskDraft", "setupSavingTaskForms"),
            ("confirmTransferDraft", "cancelTransferDraft"),
            ("confirmDebtDraft", "cancelDebtDraft"),
            ("confirmHistoryDraft", "setupHistoryForms"),
        )
        for current, following in cases:
            with self.subTest(current=current):
                source = self._function_source(current, following)
                self.assertIn('playSoundEffect("transaction-success", { eventKey:', source)
                self.assertLess(source.index(".then(function"), source.index('playSoundEffect("transaction-success"'))
        transaction_source = self._function_source("confirmTransactionDraft", "cancelTransactionDraft")
        self.assertIn('playSoundEffect("error-soft", { eventKey:', transaction_source)
        self.assertLess(transaction_source.index(".catch(function"), transaction_source.index('playSoundEffect("error-soft"'))

    def test_achievement_sound_uses_claimed_notification_event(self) -> None:
        self.assertIn('"vydno:achievement-celebration"', self.sound_source)
        self.assertIn('play("achievement-unlocked", { eventKey:', self.sound_source)
        self.assertIn('dispatchAchievementCelebration(item)', self.push_source)
        self.assertIn('notificationId: notificationId', self.push_source)
        self.assertNotIn("achievementGrid.addEventListener", self.sound_source)

    def test_voice_cues_are_bound_to_real_recorder_transitions(self) -> None:
        cue_source = self._function_source("playAiVoiceStartCue", "startAiVoiceRecording")
        source = self._function_source("startAiVoiceRecording", "setupAiVoice")
        setup_source = self._function_source("setupAiVoice", "aiImageBatchElements")
        self.assertNotIn('data-sound-on-gesture="voice-start"', self.template)
        self.assertIn('id="aiVoiceStartCue"', self.template)
        self.assertIn('preload="auto"', self.template)
        self.assertIn("{% static 'miniapp/sounds/voice-start.mp3' %}?v={{ miniapp_asset_version }}", self.template)
        self.assertIn('cue.play()', cue_source)
        self.assertIn('playSoundEffect("voice-start", { forceAudio: true', cue_source)
        self.assertIn("return new Promise(function (resolve)", cue_source)
        self.assertIn('cue.addEventListener("ended", finish, { once: true })', cue_source)
        self.assertIn("window.setTimeout(finish, 500)", cue_source)
        self.assertIn("playAiVoiceStartCue().finally(function ()", source)
        self.assertNotIn("playAiVoiceStartCue();", source)
        self.assertNotIn("event.detail === 0", setup_source)
        self.assertLess(
            source.index("playAiVoiceStartCue().finally(function ()"),
            source.index("navigator.mediaDevices.getUserMedia"),
        )
        self.assertIn('playSoundEffect("voice-stop", { eventKey:', source)
        self.assertIn('playSoundEffect("error-soft", { eventKey:', source)
        self.assertLess(source.index("releaseAiVoiceStream()"), source.index('playSoundEffect("voice-stop"'))

    def test_local_assets_and_license_registry_are_complete(self) -> None:
        expected = {
            "success.mp3",
            "achievement.mp3",
            "notify.mp3",
            "voice-start.mp3",
            "voice-stop.mp3",
            "error-soft.mp3",
        }
        self.assertEqual(set(self.registry["assets"]), expected)
        self.assertEqual({path.name for path in self.sound_dir.glob("*.mp3")}, expected)
        for filename, metadata in self.registry["assets"].items():
            path = self.sound_dir / filename
            self.assertTrue(path.exists(), filename)
            self.assertGreater(path.stat().st_size, 100, filename)
            self.assertEqual(metadata["license"], "CC0-1.0")
            self.assertTrue(metadata["source_title"])
            self.assertTrue(metadata["source_url"])
            self.assertTrue(metadata["license_url"])
            self.assertTrue(metadata["modifications"])
            self.assertEqual(metadata["channels"], 1)
            self.assertEqual(metadata["sample_rate_hz"], 44100)
            self.assertGreaterEqual(metadata["bit_rate_bps"], 64000)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), metadata["sha256"])

    def test_service_worker_precaches_sound_module_and_audio(self) -> None:
        for filename in (
            "success.mp3",
            "achievement.mp3",
            "notify.mp3",
            "voice-start.mp3",
            "voice-stop.mp3",
            "error-soft.mp3",
        ):
            self.assertIn(f'_miniapp_asset_stamp("sounds/{filename}")', self.views)
            self.assertIn(f'_versioned_static("miniapp/sounds/{filename}")', self.views)
        self.assertIn('_miniapp_asset_stamp("sounds/LICENSE.json")', self.views)
        self.assertIn('"audio/"', self.views)


if __name__ == "__main__":
    unittest.main()
