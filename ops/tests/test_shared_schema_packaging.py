from pathlib import Path
import unittest
import yaml

CODE = Path(__file__).resolve().parents[2]


class SharedSchemaPackagingTests(unittest.TestCase):
    def test_compose_uses_migrations_as_the_single_schema_job(self):
        text = (CODE / "compose.yml").read_text(encoding="utf8")
        self.assertNotIn("bootstrap_core_schema", text)
        data = yaml.safe_load(text)
        command = " ".join(data["services"]["schema"]["command"])
        self.assertIn("manage.py migrate --noinput", command)
        self.assertIn("manage.py migrate --check", command)

    def test_images_build_from_shared_context_and_copy_runtime_schema(self):
        data = yaml.safe_load((CODE / "compose.yml").read_text(encoding="utf8"))
        for service, dockerfile in (("bot", "bot/Dockerfile"), ("api", "bot/Dockerfile"), ("admin", "admin_service/Dockerfile"), ("worker", "admin_service/Dockerfile"), ("beat", "admin_service/Dockerfile"), ("schema", "admin_service/Dockerfile")):
            with self.subTest(service=service):
                self.assertEqual(data["services"][service]["build"], {"context": ".", "dockerfile": dockerfile})
        for path in (CODE / "bot/Dockerfile", CODE / "admin_service/Dockerfile"):
            self.assertIn("COPY runtime_schema /app/runtime_schema", path.read_text(encoding="utf8"))
        admin_dockerfile = (CODE / "admin_service/Dockerfile").read_text(encoding="utf8")
        self.assertIn("COPY bot/trial_recovery.py bot/admin_integrations.py /bot/", admin_dockerfile)

    def test_release_workflow_builds_images_from_the_required_contexts(self):
        workflow = (CODE / ".github/workflows/deploy.yml").read_text(encoding="utf8")

        self.assertIn('docker build --pull -t "mcf-bot:$GITHUB_SHA" -f bot/Dockerfile .', workflow)
        self.assertIn('docker build --pull -t "mcf-admin:$GITHUB_SHA" -f admin_service/Dockerfile .', workflow)
        self.assertIn('docker build --pull -t "mcf-nginx:$GITHUB_SHA" nginx', workflow)

    def test_release_validator_uses_normal_migrations_only(self):
        text = (CODE / "ops/validate_release.sh").read_text(encoding="utf8")
        self.assertNotIn("bootstrap_core_schema", text)
        self.assertNotIn("--fake-initial", text)
        self.assertGreaterEqual(text.count("manage.py migrate --noinput"), 2)


if __name__ == "__main__":
    unittest.main()
